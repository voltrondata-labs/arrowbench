# This is a reconstructed schema; it may not be completely accurate, but should
# be serviceably so. For more information on variables, see the sources below
# available from Fannie Mae. You may need to create an account first here:
# https://datadynamics.fanniemae.com/data-dynamics/#/reportMenu;category=HP
#
# Data dictionary: https://capitalmarkets.fanniemae.com/resources/file/credit-risk/xls/crt-file-layout-and-glossary.xlsx
# Names from R code here: https://capitalmarkets.fanniemae.com/media/document/zip/FNMA_SF_Loan_Performance_r_Primary.zip
fanniemae_schema <- function() {
  arrow::schema(
    LOAN_ID = arrow::string(),
    # date. Monthly reporting period
    ACT_PERIOD = arrow::string(),
    SERVICER = arrow::string(),
    ORIG_RATE = arrow::float64(),
    CURRENT_UPB = arrow::float64(),
    LOAN_AGE = arrow::int32(),
    REM_MONTHS = arrow::int32(),
    ADJ_REM_MONTHS = arrow::int32(),
    # maturity date
    MATR_DT = arrow::string(),
    # Metropolitan Statistical Area code
    MSA = arrow::string(),
    # Int of months, but `X` is a valid value. New versions pad with `0`/`X` to two characters
    DLQ_STATUS = arrow::string(),
    RELOCATION_MORTGAGE_INDICATOR = arrow::string(),
    # 0-padded 2 digit ints representing categorical levels, e.g. "01" -> "Prepaid or Matured"
    Zero_Bal_Code = arrow::string(),
    # date
    ZB_DTE = arrow::string(),
    LAST_PAID_INSTALLMENT_DATE = arrow::string(),
    FORECLOSURE_DATE = arrow::string(),
    DISPOSITION_DATE = arrow::string(),
    FORECLOSURE_COSTS = arrow::float64(),
    PROPERTY_PRESERVATION_AND_REPAIR_COSTS = arrow::float64(),
    ASSET_RECOVERY_COSTS = arrow::float64(),
    MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS = arrow::float64(),
    ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY = arrow::float64(),
    NET_SALES_PROCEEDS = arrow::float64(),
    CREDIT_ENHANCEMENT_PROCEEDS = arrow::float64(),
    REPURCHASES_MAKE_WHOLE_PROCEEDS = arrow::float64(),
    OTHER_FORECLOSURE_PROCEEDS = arrow::float64(),
    NON_INTEREST_BEARING_UPB = arrow::float64(),
    # all null
    MI_CANCEL_FLAG = arrow::string(),
    RE_PROCS_FLAG = arrow::string(),
    # all null
    LOAN_HOLDBACK_INDICATOR = arrow::string(),
    SERV_IND = arrow::string()
  )
}


#' Known data files
#' @export
known_sources <- list(
  fanniemae_2016Q4 = list(
    url = "https://ursa-qa.s3.amazonaws.com/fanniemae_loanperf/2016Q4.csv.gz",
    schema = fanniemae_schema(),
    delim = "|",
    dim = c(22180168L, 31L)
  ),
  `nyctaxi_2010-01` = list(
    url = "https://ursa-qa.s3.amazonaws.com/nyctaxi/yellow_tripdata_2010-01.csv.gz",
    delim = ",",
    dim = c(14863778L, 18L)
  ),
  chi_traffic_2020_Q1 = list(
    url = "https://ursa-qa.s3.amazonaws.com/chitraffic/chi_traffic_2020_Q1.parquet",
    dim = c(13038291L, 23L)
  ),
  type_strings = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_strings.parquet",
    dim = c(1000000L, 5L)
  ),
  type_dict = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_dict.parquet",
    dim = c(1000000L, 5L)
  ),
  type_integers = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_integers.parquet",
    dim = c(1000000L, 5L)
  ),
  type_floats = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_floats.parquet",
    dim = c(1000000L, 5L)
  ),
  type_nested = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_nested.parquet",
    dim = c(1000000L, 4L)
  ),
  type_simple_features = list(
    url = "https://ursa-qa.s3.amazonaws.com/single_types/type_simple_features.parquet",
    dim = c(1000000L, 5L)
  ),
  tpch = list(
    generator = function(...) generate_tpch(...),
    locator = function(...) ensure_tpch(...),
    dim = c(1000000L, 5L)
  )
)

# Now add the "custom" locator
# TODO: we need to exclude fanniemae_2016Q4 and nyctaxi_2010-01 for now, until CSVs are better handled
known_sources <- purrr::map2(known_sources, names(known_sources), function(source, name) {
  force(name)

  if (is.null(source$locator)) {
    source$locator <- function(...) datalogistik_locate(name)[[1]]$path
  }

  return(source)
})

# these are similar to known_sources above, with the exception that they come
# with the package, so they have a filename instead of a url
test_sources <- list(
  fanniemae_sample = list(
    # this is the first 100 lines of the ungzipped PSV
    filename = "fanniemae_sample.csv",
    schema = fanniemae_schema(),
    delim = "|",
    dim = c(100L, 31L)
  ),
  nyctaxi_sample = list(
    filename = "nyctaxi_sample.csv",
    delim = ",",
    dim = c(998L,  18L)
  ),
  chi_traffic_sample = list(
    filename = "chi_traffic_sample.parquet",
    dim = c(1000L, 23L)
  )
)

all_sources <- c(known_sources, test_sources)

taxi_schema <- function() {
  arrow::schema(
    vendor_id = arrow::string(),
    pickup_at = arrow::timestamp(unit = "us"),
    dropoff_at = arrow::timestamp(unit = "us"),
    passenger_count = arrow::int8(),
    trip_distance = arrow::float(),
    pickup_longitude = arrow::float(),
    pickup_latitude = arrow::float(),
    rate_code_id = arrow::string(),
    store_and_fwd_flag = arrow::string(),
    dropoff_longitude = arrow::float(),
    dropoff_latitude = arrow::float(),
    payment_type = arrow::string(),
    fare_amount = arrow::float(),
    extra = arrow::float(),
    mta_tax = arrow::float(),
    tip_amount = arrow::float(),
    tolls_amount = arrow::float(),
    total_amount = arrow::float()
  )
}

known_datasets <- list(
  taxi_2013 = list(
    url = "https://archive.org/download/nycTaxiTripData2013/trip_fare.7z",
    download = function(path) {
      archive_path <- file.path(dirname(path), "trip_fare.7z")
      if (!file.exists(archive_path)) {
        download.file(
          "https://archive.org/download/nycTaxiTripData2013/trip_fare.7z",
          archive_path,
          mode = 'wb',
          method = 'wget'
        )
      }

      archive::archive_extract(archive_path, dir = path)

      files <- list.files(path, pattern = '\\.csv$', full.names = TRUE)
      lapply(files, function(file) {
        message("Fixing headers in ", file)
        lines <- readr::read_lines(file)
        lines[[1]] <- gsub(", ", ",", lines[[1]], fixed = TRUE)
        readr::write_lines(lines, file)

        message("gzipping ", file)
        R.utils::gzip(file, remove = TRUE)
      })

      invisible(path)
    },
    open = function(paths) {
      arrow::open_dataset(paths, format = "csv")
    },
    dim = c(173179759L, 11L),
    n_files = 12L
  ),
  taxi_parquet = list(
    url = "s3://ursa-labs-taxi-data",
    download = function(path) {
      arrow::copy_files("s3://ursa-labs-taxi-data", path)
      invisible(path)
    },
    open = function(paths) {
      arrow::open_dataset(paths, partitioning = c("year", "month"))
    },
    dim = c(1547741381L, 20L),
    n_files = 125
  ),
  taxi_file_list_parquet = list(
    url = "s3://ursa-labs-taxi-data",
    files = c(
      "/2009/01/data.parquet",
      "/2009/02/data.parquet"
    ),
    download = function(path) {
      # TODO, find a way to do this if we ever want to download these.
      stop("Can't do that")
    },
    open = function(paths) {
      arrow::open_dataset(
        paths,
        schema = taxi_schema(),
        partitioning = c("year", "month"),
        format = "parquet"
      )
    },
    n_files = 2,
    region = "us-east-2",
    dim = c(27472535L, 18L) # TODO: fix
  ),
  taxi_file_list_feather = list(
    url = "s3://ursa-labs-taxi-data-ipc",
    files = c(
      "/2013/01/data.feather",
      "/2013/02/data.feather"
    ),
    download = function(path) {
      # TODO, find a way to do this if we ever want to download these.
      stop("Can't do that")
    },
    open = function(paths) {
      arrow::open_dataset(
        paths,
        schema = taxi_schema(),
        partitioning = c("year", "month"),
        format = "feather"
      )    },
    n_files = 2,
    region = "us-east-2",
    dim = c(28766791L, 18L) # TODO: fix
  )
)

test_datasets <- list(
  taxi_2013_sample = list(
    dirname = "taxi_2013",
    open = function() {
      arrow::open_dataset(
        system.file("test_data", "datasets", "taxi_2013", package = "arrowbench"),
        format = "csv"
      )
    },
    n_files = 12L,
    dim = c(1000L, 11L)
  ),
  write = function() {
    library(dplyr)

    ds <- ensure_source("taxi_2013")
    dir <- file.path("inst", "test_data", "datasets", "taxi_2013")
    unlink(dir, recursive = TRUE)
    dir.create(dir, recursive = TRUE)

    set.seed(47L)
    i <- sample(nrow(ds), 1000L)

    df_sample <- ds %>%
      # this dataset has a suspicious amount of duplicative data; this is all vars in order
      arrange(medallion, hack_license, vendor_id, pickup_datetime, payment_type,
              fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount) %>%
      .[i, ] %>%
      collect()

    df_sample %>%
      arrange(medallion, hack_license, vendor_id, pickup_datetime, payment_type,
              fare_amount, surcharge, mta_tax, tip_amount, tolls_amount, total_amount) %>%
      mutate(
        month = lubridate::month(pickup_datetime),
        filename = paste0("taxi_2013_", month, ".csv.gz"),
        path = file.path("inst/test_data/datasets/taxi_2013/", filename)
      ) %>%
      tidyr::nest(data = c(-month, -filename, -path)) %>%
      { purrr::walk2(.$data, .$path, ~arrow::write_csv_arrow(.x, gzfile(.y)) ) }
  }
)

all_datasets <- c(known_datasets, test_datasets)
