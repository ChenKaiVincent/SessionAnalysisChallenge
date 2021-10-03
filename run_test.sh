dir=$(pwd)
export PYTHONPATH="${PYTHONPATH}:${dir}"
python -m pytest tests/
