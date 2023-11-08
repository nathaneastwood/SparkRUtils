<a name="readme-top"></a>

# SparkRUtils

[![CRAN status](https://www.r-pkg.org/badges/version/SparkRUtils)]()

## Overview

`SparkRUtils` provides utility functions for working with the [`SparkR`](https://github.com/apache/spark/R/pkg) package.

## Installation

Before installing `SparkRUtils` you must install `SparkR` since, at the time of writing, `SparkR` is not available on CRAN. To install `SparkR` please follow these instructions:

```r
# install.packages("remotes")
remotes::install_github("apache/spark/R/pkg")
```

From there you will be able to install `SparkRUtils` with:

```r
remotes::install_github("nathaneastwood/SparkRUtils")
```

If you wish for `SparkRUtils`' documentation to have completed links, you must clone [apache/spark](https://github.com/apache/spark), build the documentation and install from source, i.e.

```bash
git clone https://github.com/apache/spark
cd spark/R/pkg
# Rscript -e "install.packages('roxygen2')"
Rscript -e "roxygen2::roxygenise()"
R CMD INSTALL .
```
