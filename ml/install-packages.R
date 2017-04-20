## Create the personal library if it doesn't exist. Ignore a warning if the directory already exists.
dir.create(Sys.getenv("R_LIBS_USER"), showWarnings = FALSE, recursive = TRUE)
## Install one package.
install.packages("doMC", Sys.getenv("R_LIBS_USER"), repos = "http://cran.case.edu" )
install.packages("glmnet", Sys.getenv("R_LIBS_USER"), repos = "http://cran.case.edu" )
install.packages("pROC", Sys.getenv("R_LIBS_USER"), repos = "http://cran.case.edu" )

## Install a package that you have copied to the remote system.
install.packages("https://cran.r-project.org/src/contrib/Archive/MIICD/MIICD_2.1.tar.gz", Sys.getenv("R_LIBS_USER"), repos=NULL, type="source")
## Install multiple packages.
## install.packages(c("timeDate","robustbase"), Sys.getenv("R_LIBS_USER"), repos = "http://cran.case.edu" )