.libPaths(c(
  "/mnt/research/Viens_AgroEco_Lab/r/brms_cmdstanr_env/lib/R/library",
  "/mnt/research/Viens_AgroEco_Lab/RLib4.3.3",
  .libPaths()
))

library(tidyverse)
library(brms)
library(cmdstanr)
library(chkptstanr)
library(posterior)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop(
    paste(
      "Usage:",
      "Rscript Neighborhood_fit_chkptstanr.R <data_csv> <checkpoint_dir>",
      "[iter_warmup] [iter_sampling] [iter_per_chkpt] [chains] [seed] [stop_after] [reset]",
      "[wall_clock_limit_seconds] [wall_clock_margin_seconds]"
    )
  )
}

dat_path <- args[1]
checkpoint_dir <- args[2]
iter_warmup <- if (length(args) >= 3) as.integer(args[3]) else 1000L
iter_sampling <- if (length(args) >= 4) as.integer(args[4]) else 1000L
iter_per_chkpt <- if (length(args) >= 5) as.integer(args[5]) else 100L
chains <- if (length(args) >= 6) as.integer(args[6]) else 2L
seed <- if (length(args) >= 7) as.integer(args[7]) else 1234L
stop_after <- if (length(args) >= 8 && nzchar(args[8])) as.integer(args[8]) else NULL
reset <- if (length(args) >= 9) as.logical(as.integer(args[9])) else FALSE
wall_clock_limit_seconds <- if (length(args) >= 10 && nzchar(args[10])) as.integer(args[10]) else NULL
wall_clock_margin_seconds <- if (length(args) >= 11 && nzchar(args[11])) as.integer(args[11]) else 600L

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

this_county_dat <- this_df %>%
  mutate(
    tillage_0_prop = as.numeric(scale(tillage_0_prop)),
    tillage_1_prop = as.numeric(scale(tillage_1_prop)),
    nccpi3corn = as.numeric(scale(nccpi3corn)),
    vpdmax_7 = as.numeric(scale(vpdmax_7)),
    year = as.numeric(year - 2010)
  ) %>%
  ungroup()

formula <- brms::bf(
  as.formula(
    paste0(
      "unscaled_yield ~ ",
      paste(model_covars, collapse = " + "),
      " + (1 | tile_field_ID)"
    )
  )
)

prior <- prior_string("normal(2250,500)", class = "Intercept")
for (i in seq_along(model_covars)) {
  this_cov <- model_covars[i]
  if (!grepl("*", this_cov, fixed = TRUE)) {
    ind <- which(this_scaling_factors$param == this_cov)
    if (length(ind) == 1) {
      scaled_sd <- this_scaling_factors$sd[ind]
      prior <- prior + prior_string(
        paste0("normal(0,", max(0.1, as.numeric(scaled_sd) * 2), ")"),
        class = "b",
        coef = this_cov
      )
    }
  }
}

message("Rows: ", nrow(this_county_dat))
message("Unique tile_field_ID: ", dplyr::n_distinct(this_county_dat$tile_field_ID))
message("Checkpoint dir: ", checkpoint_dir)
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
  fit_rds <- file.path(checkpoint_dir, "fit.rds")
  fit_summary <- file.path(checkpoint_dir, "fit_summary.csv")
  saveRDS(fit, file = fit_rds)
  readr::write_csv(
    as_tibble(as.data.frame(summary(fit)$fixed), rownames = "term"),
    fit_summary
  )
  message("Saved fit to: ", fit_rds)
  message("Saved summary to: ", fit_summary)
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
      write_status("process complete.")
    } else {
      write_status("resume to process the next batch")
    }
  } else {
    message("Run stopped before any sample chunks were available.")
    if (completed_checkpoints >= expected_checkpoints) {
      write_status("process complete.")
    } else {
      write_status("resume to process the next batch")
    }
  }
}
