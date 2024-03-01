# Description: Install packages in current python3 and update requirements.txt
# Example: sh pip_install.sh pandas numpy

python3 -m pip install "$@"
python3 -m pip freeze > requirements.txt