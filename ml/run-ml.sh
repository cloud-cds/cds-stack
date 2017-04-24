R CMD BATCH /ml/install-packages.R
alias python=python3.6
python ml/generate_input.py
python ml/main.py input/1.pkl