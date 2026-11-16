const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');

const workflow = readFileSync(
  join(__dirname, '../../.github/workflows/copilot-ready-review.yml'), 'utf8',
);
const script = workflow.split('          script: |\n')[1].replace(/^            /gm, '');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const run = new AsyncFunction('github', 'context', 'core', 'setTimeout', script);
const marker = '<!-- lakebench-copilot-first-ready:v1 -->';

function fixture(options = {}) {
  const calls = {requests: [], comments: [], updates: [], waits: [], notices: []};
  const responses = [...(options.responses ?? [{status: 201, data: {state: 'PENDING'}}])];
  const pulls = [...(options.pulls ?? [{state: 'open', draft: false}])];
  let latestPull = pulls[0];
  const github = {
    rest: {
      pulls: {
        get: async () => {
          latestPull = pulls.shift() ?? latestPull;
          return {data: latestPull};
        },
      },
      issues: {
        listComments: Symbol('listComments'),
        createComment: async parameters => {
          calls.comments.push(parameters);
          return {data: {id: 42}};
        },
        updateComment: async parameters => {
          calls.updates.push(parameters);
          if (options.updateError) throw options.updateError;
        },
      },
    },
    graphql: async () => ({
      repository: {
        pullRequest: {
          timelineItems: {
            nodes: Array.from({length: options.transitions ?? 1}, (_, id) => ({id})),
          },
        },
      },
    }),
    paginate: async () => options.comments ?? [],
    request: async (route, parameters) => {
      calls.requests.push({route, parameters});
      const response = responses.shift();
      if (response instanceof Error) throw response;
      assert.ok(response, 'Unexpected extra review request');
      return response;
    },
  };
  const core = {
    notice: message => calls.notices.push(message),
    info: () => {},
    error: () => {},
  };
  return {
    calls,
    run: () => run(
      github,
      {repo: {owner: 'microsoft', repo: 'LakeBench'}, payload: {pull_request: {number: 123}}},
      core,
      (callback, delay) => {calls.waits.push(delay); callback();},
    ),
  };
}

function pendingError() {
  return Object.assign(new Error('Already pending'), {status: 409});
}

test('requests one Balanced review at the first ready transition', async () => {
  const f = fixture();
  await f.run();
  assert.equal(f.calls.requests.length, 1);
  assert.match(f.calls.requests[0].route, /\/copilot-code-review$/);
  assert.equal(f.calls.requests[0].parameters.review_effort, 'balanced');
  assert.equal(f.calls.requests[0].parameters.pull_number, 123);
  assert.match(f.calls.comments[0].body, /\nStatus: requesting\n/);
  assert.match(f.calls.updates[0].body, /\nStatus: requested\n/);
});

test('does not request another review at subsequent ready transitions', async () => {
  const f = fixture({transitions: 2});
  await f.run();
  assert.deepEqual(f.calls.requests, []);
  assert.deepEqual(f.calls.comments, []);
});

test('fails explicitly when the triggering transition is not visible', async () => {
  const f = fixture({transitions: 0});
  await assert.rejects(f.run, /not visible/);
  assert.deepEqual(f.calls.requests, []);
});

test('a workflow rerun does not duplicate a confirmed review request', async () => {
  const f = fixture({comments: [{
    user: {login: 'github-actions[bot]', type: 'Bot'},
    body: `${marker}\nStatus: requested\n`,
  }]});
  await f.run();
  assert.deepEqual(f.calls.requests, []);
});

test('a contributor cannot suppress the review with a forged marker', async () => {
  const f = fixture({comments: [{
    user: {login: 'contributor', type: 'User'},
    body: `${marker}\nStatus: requested\n`,
  }]});
  await f.run();
  assert.equal(f.calls.requests.length, 1);
});

test('an ambiguous or failed earlier attempt requires manual intervention', async () => {
  for (const status of ['requesting', 'failed', 'cancelled']) {
    const f = fixture({comments: [{
      user: {login: 'github-actions[bot]', type: 'Bot'},
      body: `${marker}\nStatus: ${status}\n`,
    }]});
    await assert.rejects(f.run, /potentially duplicate paid request/);
    assert.deepEqual(f.calls.requests, []);
  }
});

test('waits for a draft review to finish before requesting the additional review', async () => {
  const f = fixture({responses: [
    pendingError(), {status: 201, data: {state: 'PENDING'}},
  ]});
  await f.run();
  assert.equal(f.calls.requests.length, 2);
  assert.deepEqual(f.calls.waits, [30000]);
  assert.match(f.calls.updates.at(-1).body, /\nStatus: requested\n/);
});

test('pending reviews time out visibly after a bounded wait', async () => {
  const f = fixture({responses: Array.from({length: 11}, pendingError)});
  await assert.rejects(f.run, /still pending after five minutes/);
  assert.equal(f.calls.requests.length, 11);
  assert.equal(f.calls.waits.length, 10);
  assert.match(f.calls.updates.at(-1).body, /\nStatus: failed\n/);
});

test('licensing and API failures fail without retrying paid requests', async () => {
  for (const status of [403, 422, 429, 503]) {
    const f = fixture({responses: [Object.assign(new Error('Denied'), {status})]});
    await assert.rejects(f.run, /Denied/);
    assert.equal(f.calls.requests.length, 1);
    assert.deepEqual(f.calls.waits, []);
    assert.match(f.calls.updates.at(-1).body, /\nStatus: failed\n/);
  }
});

test('does not claim success for an unexpected API response', async () => {
  const f = fixture({responses: [{status: 200, data: {state: 'UNKNOWN'}}]});
  await assert.rejects(f.run, /did not confirm/);
  assert.match(f.calls.updates.at(-1).body, /\nStatus: failed\n/);
});

test('does not review a closed PR or one returned to draft', async () => {
  for (const pull of [{state: 'closed', draft: false}, {state: 'open', draft: true}]) {
    const f = fixture({pulls: [pull]});
    await f.run();
    assert.deepEqual(f.calls.requests, []);
  }
});

test('stops if the PR returns to draft while a review is pending', async () => {
  const f = fixture({
    pulls: [
      {state: 'open', draft: false},
      {state: 'open', draft: false},
      {state: 'open', draft: true},
    ],
    responses: [pendingError()],
  });
  await f.run();
  assert.equal(f.calls.requests.length, 1);
  assert.match(f.calls.updates.at(-1).body, /\nStatus: cancelled\n/);
});

test('a failed ledger write never becomes a success-shaped result', async () => {
  const f = fixture({updateError: new Error('Comment update failed')});
  await assert.rejects(f.run, /Comment update failed/);
  assert.equal(f.calls.requests.length, 1);
});
