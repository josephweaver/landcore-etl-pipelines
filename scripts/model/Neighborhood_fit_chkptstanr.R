.libPaths(c(
  "/mnt/research/Viens_AgroEco_Lab/r/brms_cmdstanr_env/lib/R/library",
  "/mnt/research/Viens_AgroEco_Lab/RLib4.3.3",
  .libPaths()
))

# Neighborhood_fit_chkptstanr.R
#
# Purpose:
# Fit a Gaussian brms model for field-level yield using chkptstanr so a long
# Stan run can be checkpointed, resumed, and partially summarized if needed.
#
# Required inputs:
# 1. <data_csv>
#    Path to a CSV file containing the modeling dataset. The script expects at
#    least these columns:
#    - unscaled_yield
#    - tile_field_ID
#    - tillage_0_prop
#    - tillage_1_prop
#    - nccpi3corn
#    - vpdmax_7
#    - year
# 2. <checkpoint_dir>
#    Directory where checkpoint files and final outputs will be written.
#
# Optional command-line inputs:
# 3. [output_model_rds]
#    Path where the final fitted brms model is saved when all checkpoints finish.
# 4. [fit_summary_json]
#    Path for a JSON summary describing the fit inputs, status, and outputs.
# 5. [iter_warmup]
# 6. [iter_sampling]
# 7. [iter_per_chkpt]
# 8. [chains]
# 9. [seed]
# 10. [stop_after]
# 11. [reset]
# 12. [wall_clock_limit_seconds]
# 13. [wall_clock_margin_seconds]
#
# Main outputs:
# - output_model_rds when all checkpoints are complete
# - fit_summary_json with run metadata and status
# - checkpoint_dir/run_status.txt with a simple completion/resume status message
# - checkpoint_dir/cp_info/ and checkpoint_dir/cp_samples/ created by chkptstanr
# - checkpoint_dir/fit.rds when all checkpoints are complete
# - checkpoint_dir/fit_summary.csv when all checkpoints are complete
# - checkpoint_dir/checkpoint_draws.rds when partial checkpoint draws can be combined
# - checkpoint_dir/checkpoint_draws_summary.csv when partial checkpoint draws can be summarized
#
# Checkpoint-specific outputs written to <checkpoint_dir>:
# - cp_info/ and cp_samples/ checkpoint files created by chkptstanr
# - run_status.txt with a simple completion/resume status message
# - fit.rds when all checkpoints are complete
# - fit_summary.csv when all checkpoints are complete
# - checkpoint_draws.rds when partial checkpoint draws can be combined
# - checkpoint_draws_summary.csv when partial checkpoint draws can be summarized
#
# Behavior:
# - Standardizes key numeric predictors before fitting
# - Fits a model with fixed effects plus a random intercept for tile_field_ID
# - Stops early when close to a wall-clock limit so the run can resume later
# - Saves either a final fit or the best available partial posterior summary

library(tidyverse)
library(brms)
library(cmdstanr)
library(chkptstanr)
library(posterior)
library(jsonlite)

# This script is designed to run from the command line inside a batch job.
# The first two arguments are required; the rest let the caller tune runtime behavior.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop(
    paste(
      "Usage:",
      "Rscript Neighborhood_fit_chkptstanr.R <data_csv> <checkpoint_dir> [output_model_rds] [fit_summary_json]",
      "[iter_warmup] [iter_sampling] [iter_per_chkpt] [chains] [seed] [stop_after] [reset]",
      "[wall_clock_limit_seconds] [wall_clock_margin_seconds]"
    )
  )
}

dat_path <- args[1]
checkpoint_dir <- args[2]
output_model_rds <- if (length(args) >= 3 && nzchar(args[3])) args[3] else file.path(dirname(checkpoint_dir), "output_model")
fit_summary_json <- if (length(args) >= 4 && nzchar(args[4])) args[4] else file.path(dirname(checkpoint_dir), "fit_summary.json")
iter_warmup <- if (length(args) >= 5) as.integer(args[5]) else 250L
iter_sampling <- if (length(args) >= 6) as.integer(args[6]) else 2500L
iter_per_chkpt <- if (length(args) >= 7) as.integer(args[7]) else 250L
chains <- if (length(args) >= 8) as.integer(args[8]) else 3L
seed <- if (length(args) >= 9) as.integer(args[9]) else 1234L
stop_after <- if (length(args) >= 10 && nzchar(args[10])) as.integer(args[10]) else NULL
reset <- if (length(args) >= 11) as.logical(as.integer(args[11])) else FALSE
wall_clock_limit_seconds <- if (length(args) >= 12 && nzchar(args[12])) as.integer(args[12]) else NULL
wall_clock_margin_seconds <- if (length(args) >= 13 && nzchar(args[13])) as.integer(args[13]) else 600L

# chkptstanr saves progress in chunks, so we can resume a long Stan run later.
expected_checkpoints <- as.integer(ceiling((iter_warmup + iter_sampling) / iter_per_chkpt))
status_file <- file.path(checkpoint_dir, "run_status.txt")

write_status <- function(status_text) {
  writeLines(status_text, status_file)
  message(status_text)
}

wall_clock_start <- Sys.time()
wall_clock_deadline <- if (is.null(wall_clock_limit_seconds)) NULL else wall_clock_start + wall_clock_limit_seconds
wall_clock_remaining <- function() {
  if (is.null(wall_clock_deadline)) {
    return(Inf)
  }
  as.numeric(difftime(wall_clock_deadline, Sys.time(), units = "secs"))
}

should_stop_for_time <- function() {
  if (is.null(wall_clock_deadline)) {
    return(FALSE)
  }
  wall_clock_remaining() <= wall_clock_margin_seconds
}

dir.create(checkpoint_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(output_model_rds), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(fit_summary_json), recursive = TRUE, showWarnings = FALSE)

# Try a few known CmdStan locations so the script can run on shared infrastructure
# without requiring every caller to set CMDSTAN manually.
cmdstan_candidates <- c(
  Sys.getenv("CMDSTAN", unset = ""),
  "/mnt/research/Viens_AgroEco_Lab/r/brms_cmdstanr_env/cmdstan/current",
  "/mnt/research/Viens_AgroEco_Lab/.cmdstan/cmdstan-2.35.0"
)
cmdstan_candidates <- cmdstan_candidates[nzchar(cmdstan_candidates)]
cmdstan_path_to_use <- cmdstan_candidates[file.exists(cmdstan_candidates)][1]
if (is.na(cmdstan_path_to_use) || !nzchar(cmdstan_path_to_use)) {
  stop("No CmdStan installation found.")
}
set_cmdstan_path(cmdstan_path_to_use)

this_df <- read_csv(dat_path, show_col_types = FALSE)

# These are the fixed-effect terms included in the regression.
# Interaction terms are written the way brms expects them in the formula.
model_covars <- c(
  "tillage_0_prop",
  "tillage_1_prop",
  "nccpi3corn",
  "vpdmax_7",
  "tillage_0_prop*vpdmax_7",
  "tillage_1_prop*vpdmax_7",
  "tillage_0_prop*nccpi3corn",
  "tillage_1_prop*nccpi3corn",
  "year"
)

# Match the older neighborhood scripts by using informed priors for the
# main effects while leaving interactions on the default brms priors.
priors_unscaled <- data.frame(
  parameter = c("year", "tillage_0_prop", "tillage_1_prop", "nccpi3corn", "vpdmax_7"),
  scalar_par = c("year", "tillage_0_prop", "tillage_1_prop", "nccpi3corn", "vpdmax_7"),
  linear_effect_yieldscale = c(15, 26, 26, 1150, -20)
)

# Keep the original scaling values so priors can roughly match the scale
# of each main-effect predictor before standardization.
this_scaling_factors <- data.frame(
  param = c("tillage_0_prop", "tillage_1_prop", "nccpi3corn", "vpdmax_7", "year"),
  mean = c(
    mean(this_df$tillage_0_prop),
    mean(this_df$tillage_1_prop),
    mean(this_df$nccpi3corn),
    mean(this_df$vpdmax_7),
    0
  ),
  sd = c(
    sd(this_df$tillage_0_prop),
    sd(this_df$tillage_1_prop),
    sd(this_df$nccpi3corn),
    sd(this_df$vpdmax_7),
    1
  )
)

# Standardizing numeric predictors usually makes HMC sampling more stable.
# Year is re-centered so the intercept is interpretable near the first study year.
this_county_dat <- this_df %>%
  mutate(
    tillage_0_prop = as.numeric(scale(tillage_0_prop)),
    tillage_1_prop = as.numeric(scale(tillage_1_prop)),
    nccpi3corn = as.numeric(scale(nccpi3corn)),
    vpdmax_7 = as.numeric(scale(vpdmax_7)),
    year = as.numeric(year - 2010)
  ) %>%
  drop_na(
    unscaled_yield,
    tile_field_ID,
    tillage_0_prop,
    tillage_1_prop,
    nccpi3corn,
    vpdmax_7,
    year
  ) %>%
  ungroup()

if (nrow(this_county_dat) == 0) {
  stop("county data has zero rows after dropping NA model fields")
}

# The model uses field-level random intercepts so repeated observations from the
# same field can share information without being treated as independent.
formula <- brms::bf(
  as.formula(
    paste0(
      "unscaled_yield ~ ",
      paste(model_covars, collapse = " + "),
      " + (1 | tile_field_ID)"
    )
  )
)

# Use a domain-informed prior for the intercept and weakly informative priors
# for main effects. Interaction terms keep brms defaults here.
prior <- prior_string("normal(2250,500)", class = "Intercept")
for (i in seq_along(model_covars)) {
  this_cov <- model_covars[i]
  if (!grepl("*", this_cov, fixed = TRUE) && this_cov %in% priors_unscaled$parameter) {
    ind <- which(priors_unscaled$parameter == this_cov)
    if (length(ind) == 1) {
      scaled_sd <- this_scaling_factors$sd[
        this_scaling_factors$param == priors_unscaled$scalar_par[ind]
      ]
      scaled_val <- priors_unscaled$linear_effect_yieldscale[ind] * scaled_sd
      prior <- prior + prior_string(
        paste0("normal(", scaled_val, ",", abs(scaled_val), ")"),
        class = "b",
        coef = this_cov
      )
    }
  }
}

message("Rows: ", nrow(this_county_dat))
message("Unique tile_field_ID: ", dplyr::n_distinct(this_county_dat$tile_field_ID))
message("Checkpoint dir: ", checkpoint_dir)
message("Output model path: ", output_model_rds)
message("Fit summary path: ", fit_summary_json)
message(
  "Config: iter_warmup=", iter_warmup,
  ", iter_sampling=", iter_sampling,
  ", iter_per_chkpt=", iter_per_chkpt,
  ", chains=", chains,
  ", seed=", seed,
  ", stop_after=", if (is.null(stop_after)) "NULL" else stop_after,
  ", reset=", reset,
  ", wall_clock_limit_seconds=", if (is.null(wall_clock_limit_seconds)) "NULL" else wall_clock_limit_seconds,
  ", wall_clock_margin_seconds=", wall_clock_margin_seconds
)
message("Expected checkpoints: ", expected_checkpoints)

write_fit_summary <- function(status_text, final_fit_path = NULL, partial_draws_path = NULL, partial_summary_path = NULL) {
  fit_summary <- list(
    input_csv = normalizePath(dat_path, winslash = "/", mustWork = FALSE),
    checkpoint_dir = normalizePath(checkpoint_dir, winslash = "/", mustWork = FALSE),
    output_model_rds = normalizePath(output_model_rds, winslash = "/", mustWork = FALSE),
    fit_summary_json = normalizePath(fit_summary_json, winslash = "/", mustWork = FALSE),
    row_count = nrow(this_county_dat),
    unique_tile_field_count = dplyr::n_distinct(this_county_dat$tile_field_ID),
    focal_fips = sort(unique(this_county_dat$FIPS)),
    model_covars = model_covars,
    scaling = this_scaling_factors,
    completed_checkpoints = completed_checkpoints,
    expected_checkpoints = expected_checkpoints,
    status = status_text,
    final_fit_path = if (is.null(final_fit_path)) NULL else normalizePath(final_fit_path, winslash = "/", mustWork = FALSE),
    partial_draws_path = if (is.null(partial_draws_path)) NULL else normalizePath(partial_draws_path, winslash = "/", mustWork = FALSE),
    partial_summary_path = if (is.null(partial_summary_path)) NULL else normalizePath(partial_summary_path, winslash = "/", mustWork = FALSE),
    note = "Checkpointed neighborhood fit uses tillage proportion covariates in place of the older RCI terms."
  )
  write_json(fit_summary, fit_summary_json, auto_unbox = TRUE, pretty = TRUE)
}

# stop_after controls how far chkptstanr is allowed to run in this invocation.
# We increase it one checkpoint at a time so long jobs can stop cleanly near the
# scheduler wall-clock limit and resume later.
current_stop_after <- if (is.null(stop_after)) expected_checkpoints * iter_per_chkpt else stop_after
fit <- NULL
repeat {
  if (should_stop_for_time()) {
    message("Stopping before next checkpoint chunk to preserve wall-clock margin.")
    break
  }
  next_stop_after <- min(current_stop_after, expected_checkpoints * iter_per_chkpt)
  message(
    "Starting chkpt_brms with stop_after=", next_stop_after,
    ", wall_clock_remaining_seconds=", round(wall_clock_remaining(), 1)
  )
  fit <- tryCatch(
    chkptstanr::chkpt_brms(
      formula = formula,
      data = this_county_dat,
      family = gaussian(),
      prior = prior,
      iter_adaptation = max(20L, min(150L, iter_per_chkpt)),
      iter_warmup = iter_warmup,
      iter_sampling = iter_sampling,
      iter_per_chkpt = iter_per_chkpt,
      parallel_chains = min(chains, 4L),
      threads_per = 1L,
      chkpt_progress = TRUE,
      control = list(adapt_delta = 0.90, max_treedepth = 10),
      seed = seed,
      stop_after = next_stop_after,
      reset = reset,
      path = checkpoint_dir
    ),
    error = function(e) {
      # Returning NULL lets the script exit gracefully and preserve any
      # checkpoint files that were already written.
      message("chkpt_brms error: ", conditionMessage(e))
      NULL
    }
  )
  if (is.null(fit)) {
    break
  }
  completed_checkpoints <- length(list.files(file.path(checkpoint_dir, "cp_info"), pattern = "^cp_info_[0-9]+\\.rds$", full.names = TRUE))
  if (completed_checkpoints >= expected_checkpoints) {
    break
  }
  if (should_stop_for_time()) {
    break
  }
  current_stop_after <- min(current_stop_after + iter_per_chkpt, expected_checkpoints * iter_per_chkpt)
}

completed_checkpoints <- length(list.files(file.path(checkpoint_dir, "cp_info"), pattern = "^cp_info_[0-9]+\\.rds$", full.names = TRUE))
if (!is.null(fit) && completed_checkpoints >= expected_checkpoints) {
  # When every checkpoint is present, save the full fit object plus a compact
  # coefficient summary that is easy to inspect downstream.
  fit_rds <- file.path(checkpoint_dir, "fit.rds")
  fit_summary <- file.path(checkpoint_dir, "fit_summary.csv")
  saveRDS(fit, file = fit_rds)
  saveRDS(fit, file = output_model_rds)
  readr::write_csv(
    as_tibble(as.data.frame(summary(fit)$fixed), rownames = "term"),
    fit_summary
  )
  message("Saved fit to: ", fit_rds)
  message("Saved fit to legacy output path: ", output_model_rds)
  message("Saved summary to: ", fit_summary)
  write_fit_summary("process complete.", final_fit_path = output_model_rds)
  write_status("process complete.")
} else {
  sample_files <- list.files(file.path(checkpoint_dir, "cp_samples"), full.names = TRUE)
  cp_info_files <- list.files(
    file.path(checkpoint_dir, "cp_info"),
    pattern = "^cp_info_[0-9]+\\.rds$",
    full.names = TRUE
  )
  completed_checkpoints <- length(cp_info_files)
  if (length(sample_files) > 0) {
    # If the run stopped midstream, we can still combine the checkpoint draws
    # collected so far and summarize the partial posterior.
    empty_fit <- brms::brm(
      formula = formula,
      data = this_county_dat,
      family = gaussian(),
      prior = prior,
      empty = TRUE
    )
    empty_fit$path <- checkpoint_dir
    draws <- chkptstanr::combine_chkpt_draws(empty_fit)
    draws_rds <- file.path(checkpoint_dir, "checkpoint_draws.rds")
    draws_summary <- file.path(checkpoint_dir, "checkpoint_draws_summary.csv")
    saveRDS(draws, file = draws_rds)
    draws_tbl <- posterior::summarise_draws(
      draws,
      mean,
      sd,
      ~posterior::quantile2(.x, probs = c(0.05, 0.95))
    ) %>%
      as_tibble()
    readr::write_csv(draws_tbl, draws_summary)
    message("Saved combined checkpoint draws to: ", draws_rds)
    message("Saved checkpoint draws summary to: ", draws_summary)
    if (completed_checkpoints >= expected_checkpoints) {
      write_fit_summary(
        "process complete.",
        partial_draws_path = draws_rds,
        partial_summary_path = draws_summary
      )
      write_status("process complete.")
    } else {
      write_fit_summary(
        "resume to process the next batch",
        partial_draws_path = draws_rds,
        partial_summary_path = draws_summary
      )
      write_status("resume to process the next batch")
    }
  } else {
    message("Run stopped before any sample chunks were available.")
    if (completed_checkpoints >= expected_checkpoints) {
      write_fit_summary("process complete.")
      write_status("process complete.")
    } else {
      write_fit_summary("resume to process the next batch")
      write_status("resume to process the next batch")
    }
  }
}
