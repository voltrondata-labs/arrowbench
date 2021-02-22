# arrowbench

This R package contains tools for defining benchmarks, running them across a 
range of parameters, and reporting their results in a standardized form. It also
contains some benchmark code for measuring performance of Apache Arrow and other 
projects one might compare it to.

The purpose of the package is to provide developers with better tools for 
creating, parametrizing, and reproducing benchmarks across a range of library 
versions, variables, and machines, as well as to facilitate continuous monitoring. 
While this package could be used for microbenchmarking, it is designed specially
for "macrobenchmarks": workflows that real users do with real data that take 
longer than microseconds to run. 

It builds on top of existing R benchmarking tools, notably the `bench` package.
Among the features that this package adds are

* Setup designed with parametrization in mind so you can test across a range of
  variables, which may not all be valid in combination
* Isolation of benchmark runs in separate processes to prevent cross-contamination
  such as global environment changes and previous memory allocation
* Tools for bootstrapping package versions and known data sources to facilitate
  running the same code on different machines

# User guide

## Installation

The quickest and easiest way to install is to run 
`remotes::install_github("ursa-labs/arrowbench")` in R. If you need to install 
remotes you can `install.packages("remotes")`.

If you've downloaded the source, or you're making changes to arrow bench you 
should make sure that you have the dependencies with `remotes::install_deps(".", dependencies = TRUE)` 
in R (this will also install the arrow package along with other packages that 
can be benchmarked with arrowbench. And then running `R CMD INSTALL .` in a 
terminal (for both, you should do this in the root directory of arrowbench, or 
pass the path to arrowbench instead of `.`).

## Defining benchmarks

Benchmarks are constructed by `Benchmark()`, which takes expressions that handle 
setup, teardown, and the actual work that we want to measure. See its 
documentation for details, and see `read_file` and `write_file` for examples.

## Running benchmarks

Pass a Benchmark to `run_benchmark()` and it will run it across the range of 
parameters specified. For parameters specified in `bm$setup` that are omitted
when calling `run_benchmark(bm)`, it will test across all combinations of them. 
If some parameter combinations are not valid, define a 
`bm$valid_params(params)` function that will filter that expanded `data.frame` 
of parameters down to the valid set.

For example,

```r
library(arrowbench)

run_benchmark(write_file, source = "nyctaxi_2010-01")
```

will run the `write_file` benchmark with "nyctaxi_2010-01" source file on the 
Cartesian product of the other function parameters--`format`, `compression`, and
`input`--along with `cpu_count`s of `c(1, Ncpus)`. 

Another example:

```r
library(arrowbench)

run_benchmark(write_file, source = "nyctaxi_2010-01", writer = "feather", 
  input = "data.frame", cpu_count = c(1, 4, 8))
```

will run only the Feather writing tests with the two valid compression variants,
each one done for 1, 4, and 8 threads, for a total of 6 runs.

If `lib_path` is not provided to `run_benchmark()`, it will use the default
`.libPath` and whatever is installed there. You can also indicate a subset of
released `x.y` Arrow version numbers, or `lib_path = "all"` to test all past
releases of `arrow` plus `"latest"`.

### Run options

`run_benchmark()` handles executing benchmarks across a range of parameters.
After determining the valid parameters, it calls `run_one()` on each and 
collects the results. `run_one()` generates an R script and then shells out
to a separate R process to execute the benchmark, then collects the results
from it.

You may call `run_one()` directly. It take some options, which may be passed
from `run_benchmark()` (both default `FALSE`):

* `dry_run`: logical, returns the R script instead of executing it. Useful for
  debugging, though you probably don't want to execute the script yourself in
  order to do the benchmarking: `run_script()`, which `run_one()` calls when
  `dry_run = FALSE`, has some useful wrapping for caching and collecting results.
* `profiling`: logical, allows you you instrument the R code and collect 
  profiling data. You don't want to do this if you're collecting benchmark data
  because the profiler can add to the run time, but if you see something slow
  and want to explore why, this can be a good start. Note that this doesn't do 
  profiling at the C++ level, so if something is slow inside an arrow C++ 
  function, this won't tell you what exactly, but it can help rule things out.
  If `TRUE`, the result data will contain a `prof_file` field, which you can 
  read in with `profvis::profvis(prof_input = file)`. 

## Known data sources and versions

The package knows about certain large data files to use in benchmarks. These
are registered in a `known_sources` object, which specifies where they can
be downloaded and how to read them, as well as optional attributes about them
(e.g. `dim()`) that can be used to validate that they've been read in correctly.

To use them in benchmarks, use the `ensure_source()` function to take a source
identifier and mapping it to a file path, downloading and extracting the file
if it isn't found. Pass the result to `read_source()` load the data with the
source's provided `reader` function. 

Source files are cached in a `source_data` directory and are only downloaded if
not present. This speeds up repeat benchmark runs on the same host. By default,
`source_data` is assumed to be relative to the current working directory, but
you can set `options(arrowbench.local_dir)` to point to another (permanent)
base directory.

Similarly, there is an `ensure_lib()` function called in the `global_setup()`
that supports a list of known `arrow` package versions, which are mapped to
daily snapshots of CRAN hosted by Microsoft. If you specify `lib_path = "0.17"`, 
for example, `ensure_lib()` will use a `.libPath` for this version and install
all Suggested packages into that directory using the MRAN snapshot for
"2020-05-29", a date when 0.17 was the `arrow` version on CRAN. This lets you
test against old versions of the code and to backfill benchmark results.

These versioned R package libraries are cached in an `r_libs` directory, 
like `source_data` relative to `getOption("arrowbench.local_dir", ".")`.

## Results and caching

`run_benchmark()` returns a list of benchmark results, which may be massaged,
JSON-serialized, and uploaded to the conbench service. Within an R process,
you can call `as.data.frame()` on it to get a more manageable view, which
can be passed to plotting functions.

In addition to timings, parameter values, and the versions of loaded packages,
the benchmark results contain some extra data on memory usage and garbage
collection. `gc()` can add significant time to large operations, and while we
can't prevent it, we can at least be aware of when it is happening.

Individual benchmark results (the output of `run_one()`) are cached in a
`results` directory. This way, if the main process running `run_benchmark()`
fails or is interrupted in the middle, you can restart. Note however that
if you are using the default `lib_path` **and** are updating the package
versions installed there between benchmark runs, you should clear the cache
before starting a new run (at least deleting the cached .json files containing
"latest" in the file name).
