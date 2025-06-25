# clearpandas
Code sample to write highly readable pandas code

```bash
python -m pip install virtualenv pip-tools
pip-compile -v --rebuild -o requirements.txt
pip-sync requirements.txt
python -m ipykernel install --user --name=clearpandas-kernel
```