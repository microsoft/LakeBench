{
  description = "LakeBench dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python313;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            python
            pkgs.uv
            pkgs.ruff
          ];

          shellHook = ''
            create-venv() {
              rm -rf .venv
              uv venv --python ${python}/bin/python .venv
              source .venv/bin/activate

              uv pip install -e .
              # spark and sail conflict with each other, install separately
              for extra in duckdb polars daft tpcds_datagen tpch_datagen sparkmeasure spark sail; do
                uv pip install "lakebench[$extra]" 2>&1 || echo "warning: $extra failed to install, skipping"
              done
              uv pip install --group dev
              uv pip install jupyter ipykernel
              python -m ipykernel install --user --name python3 --display-name "Python 3 (LakeBench)"

              # store hash to detect pyproject.toml changes
              md5sum pyproject.toml > .venv/.pyproject.hash
            }

            if [ ! -d .venv ]; then
              create-venv
            elif ! md5sum --check .venv/.pyproject.hash &>/dev/null; then
              echo "pyproject.toml changed, recreating venv..."
              create-venv
            else
              source .venv/bin/activate
            fi
          '';
        };
      }
    );
}
