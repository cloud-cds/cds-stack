## Create the personal library if it doesn't exist. Ignore a warning if the directory already exists.
dir.create(Sys.getenv("R_LIBS_USER"), showWarnings = FALSE, recursive = TRUE)
## Install one package.
install.packages("doMC", Sys.getenv("R_LIBS_USER"), repos = "http://cran.case.edu" )
install.packages("https://cran.r-project.org/src/contrib/Archive/glmnet/glmnet_2.0-2.tar.gz", Sys.getenv("R_LIBS_USER"), repos = NULL, type="source" )
install.packages("https://cran.r-project.org/src/contrib/Archive/pROC/pROC_1.8.tar.gz", Sys.getenv("R_LIBS_USER"), repos = NULL, type="source" )

## Install a package that you have copied to the remote system.
install.packages("https://cran.r-project.org/src/contrib/Archive/mstate/mstate_0.2.7.tar.gz", Sys.getenv("R_LIBS_USER"), repos=NULL, type="source")
install.packages("https://cran.r-project.org/src/contrib/Archive/MASS/MASS_7.3-35.tar.gz", Sys.getenv("R_LIBS_USER"), repos=NULL, type="source")
install.packages("https://cran.r-project.org/src/contrib/Archive/survival/survival_2.37-7.tar.gz", Sys.getenv("R_LIBS_USER"), repos=NULL, type="source")
install.packages("https://cran.r-project.org/src/contrib/Archive/MIICD/MIICD_2.1.tar.gz", Sys.getenv("R_LIBS_USER"), repos=NULL, type="source")

## Install multiple packages.
## install.packages(c("timeDate","robustbase"), Sys.getenv("R_LIBS_USER"), repos = "http://cran.case.edu" )