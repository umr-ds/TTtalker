# ttcloud impersonator

1. Install project to python-path
    - You might want to use a virtualenv
    - You might also want to do an editable install with `pip3 install -e .`
2. Install development dependencies with `pip3 install -r requirements.txt`
3. Install git pre-commit hook with `pre-commit install`
4. For testing: In the project root run `pytest --cov=ttcloud`