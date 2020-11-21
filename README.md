# conbench

This R package contains tools for defining benchmarks, running them across a range of parameters, and reporting their results in a language-independent form.

## Defining benchmarks

Benchmarks are constructed by `Benchmark()`, which takes functions that handle setup, teardown, and the actual work that we want to measure.

## Parametrizing benchmarks

When we benchmark, we often want to run our code compared with someone else's code, or we want to run our code but with different settings. There are a few types of parameters that get expressed differently.

### 1. Function parameters

Most things you would think of varying in a benchmark are the function being called and the arguments passed to it. These function parameters are generally specific to the function being measured, not shared across all benchmarks. 

These parameters are the arguments to the benchmark function. They should be simple values (strings or numbers) that can easily be expressed in a configuration object or file. Things like R functions that are parameters should be mapped to string identifiers.

In the benchmark function, you can specify the set of valid parameter values as default arguments. Not all parameters will have defaults, but for those that do, this enables us to document the available options *and* provide a definition of the parameter space to test.

### 2. R session parameters

Some parameters are about the global R environment, not the function being benchmarked. They may be generally applicable. These don't need to be defined and passed into every benchmark function. 

Currently supported R session parameters:

* `lib_path`: To test different library versions, install into different lib directories and provide the directories as the `lib_path` parameters. They will be passed to `.libPaths()` at the beginning of each run. 
* `cpu_count`: To restrict the number of threads available for computation, specify an integer `cpu_count`. This sets the R `Ncpus` option, which many packages follow, and also caps the `arrow` threadpool size.

Because these parameters can alter the global session state in unpredictable ways, when we run benchmarks, we always do so by calling out to a fresh R subprocess. That way, there is no potential contamination.

### 3. Environment variables



### 4. System configuration

Other parametrization you might want to explore, such as machine type, operating system, etc. is out of scope of this package. 


## Running benchmarks

Pass a Benchmark to `run_benchmark()` and it will run it across the range of parameters specified. For parameters that are omitted, if a range of values is specified as the default function argument, `run_benchmark()` will test all of them. If all parameter combinations are not valid, define a `bm$valid_params(params)` function that will filter that expanded `data.frame` of parameters down to the valid set.

For example,

```r
run_benchmark(write_file, input = "nyc-taxi.csv")
```

will run the `write_file` benchmark with "nyc-taxi.csv" on the Cartesian product of the other function parameters: format "parquet", "feather", and "fst" crossed by the three compression libraries, except "snappy" is only supported in Parquet, so this is a total of 9 runs.

Another example:

```r
run_benchmark(write_file, input = "nyc-taxi.csv", writer = "feather", cpu_count = c(1, 4, 8))
```

will run only the Feather writing tests with the two valid compression variants, each one done for 1, 4, and 8 threads, for a total of 6 runs.

## Continuous benchmarking

To monitor our performance over time and prevent performance regressions, we want to run these benchmarks with a subset of the possible parameters (e.g., we don't need to compare our code to other libraries on every commit).

The package includes a single function that runs all benchmarks in the package, following a configuration object included in the package. 

```r
run_all()
```

You can provide an alternative config object and pass it to `run_all()` to run a different subset.