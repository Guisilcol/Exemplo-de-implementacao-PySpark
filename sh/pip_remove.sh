# Description: Removes packages in current python3 and update requirements.txt
# Example: sh pip_remove.sh pandas numpy

python3 -m pip remove "$@"
python3 -m pip freeze > requirements.txt