echo "start trews-ml"
echo "installing R packages"
R CMD BATCH /ml/install-packages.R
echo "R packages installed"
alias python=python3.6
python ml/generate_input.py
python ml/main.py input/$dataset_id.pkl
