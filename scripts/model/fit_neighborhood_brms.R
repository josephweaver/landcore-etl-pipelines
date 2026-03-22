args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 1) {
  stop("usage: Rscript fit_neighborhood_brms.R <normalized_county_csv> [output_model_rds] [fit_summary_json]")
}

input_csv <- args[[1]]
output_model_rds <- if (length(args) >= 2) args[[2]] else file.path(getwd(), "output_model.rds")
fit_summary_json <- if (length(args) >= 3) args[[3]] else file.path(getwd(), "fit_summary.json")

suppressPackageStartupMessages({
  library(tidyverse)
  library(brms)
  library(jsonlite)
})

required_cols <- c(
  "tile_field_ID",
  "FIPS",
  "year",
  "unscaled_yield",
  "annual_tillage",
  "NCCPI",
  "vpdmax_7"
)

this_df <- readr::read_csv(input_csv, show_col_types = FALSE)
missing_cols <- setdiff(required_cols, colnames(this_df))
if (length(missing_cols) > 0) {
  stop(paste("missing required columns:", paste(missing_cols, collapse = ", ")))
}

fit_onecounty_brms <- function(
  county_dat,
  use_informed_priors = FALSE,
  covariate_vec,
  scaling_factors,
  ni = 2750,
  nb = 250,
  nc = 3,
  nt = 1
) {
  formula <- as.formula(paste0(
    "unscaled_yield ~ ",
    paste(covariate_vec, collapse = " + "),
    " + (1 | tile_field_ID)"
  ))

  if (use_informed_priors) {
    prior <- prior_string("normal(2250,500)", class = "Intercept")
    for (i in seq_along(covariate_vec)) {
      this_cov <- covariate_vec[[i]]
      if (this_cov %in% priors_unscaled$parameter) {
        ind <- which(priors_unscaled$parameter == this_cov)
        scaled_val <- priors_unscaled$linear_effect_yieldscale[ind] *
          scaling_factors$sd[scaling_factors$param == priors_unscaled$scalar_par[ind]]
        prior <- prior + prior_string(
          paste0("normal(", scaled_val, ",", abs(scaled_val), ")"),
          class = "b",
          coef = priors_unscaled$parameter[ind]
        )
      }
    }
  } else {
    prior <- NULL
  }

  brm(
    formula,
    data = county_dat,
    iter = ni,
    warmup = nb,
    chains = nc,
    thin = nt,
    cores = min(nc, 4),
    prior = prior
  )
}

model_covars <- c(
  "mean_tillage",
  "within_tillage",
  "NCCPI",
  "vpdmax_7",
  "within_tillage:vpdmax_7",
  "mean_tillage:vpdmax_7",
  "mean_tillage:NCCPI",
  "year"
)

priors_unscaled <- data.frame(
  parameter = c("year", "within_tillage", "mean_tillage", "NCCPI", "vpdmax_7"),
  scalar_par = c("year", "annual_tillage", "annual_tillage", "NCCPI", "vpdmax_7"),
  linear_effect_yieldscale = c(15, 26, 26, 1150, -20)
)

this_scaling_factors <- data.frame(
  param = c("annual_tillage", "vpdmax_7", "NCCPI", "year"),
  mean = c(
    mean(this_df$annual_tillage, na.rm = TRUE),
    mean(this_df$vpdmax_7, na.rm = TRUE),
    mean(this_df$NCCPI, na.rm = TRUE),
    0
  ),
  sd = c(
    sd(this_df$annual_tillage, na.rm = TRUE),
    sd(this_df$vpdmax_7, na.rm = TRUE),
    sd(this_df$NCCPI, na.rm = TRUE),
    1
  )
)

this_county_dat <- this_df %>%
  mutate(
    annual_tillage = as.numeric(scale(annual_tillage)),
    vpdmax_7 = as.numeric(scale(vpdmax_7)),
    NCCPI = as.numeric(scale(NCCPI)),
    year = as.numeric(year - 2010)
  ) %>%
  group_by(tile_field_ID) %>%
  mutate(
    mean_tillage = mean(annual_tillage, na.rm = TRUE),
    within_tillage = annual_tillage - mean(annual_tillage, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  drop_na(unscaled_yield, annual_tillage, NCCPI, vpdmax_7, year, mean_tillage, within_tillage)

if (nrow(this_county_dat) == 0) {
  stop("normalized county data has zero rows after dropping NA model fields")
}

this_county_fit <- fit_onecounty_brms(
  this_county_dat,
  use_informed_priors = TRUE,
  covariate_vec = model_covars,
  scaling_factors = this_scaling_factors
)

saveRDS(this_county_fit, file = output_model_rds)

fit_summary <- list(
  input_csv = normalizePath(input_csv, winslash = "/", mustWork = FALSE),
  output_model_rds = normalizePath(output_model_rds, winslash = "/", mustWork = FALSE),
  row_count = nrow(this_county_dat),
  unique_tile_field_count = dplyr::n_distinct(this_county_dat$tile_field_ID),
  focal_fips = sort(unique(this_county_dat$FIPS)),
  model_covars = model_covars,
  scaling = this_scaling_factors,
  note = "Neighborhood fit uses tillage decomposition (mean_tillage, within_tillage) in place of RCI decomposition."
)

write_json(fit_summary, fit_summary_json, auto_unbox = TRUE, pretty = TRUE)
cat(toJSON(fit_summary, auto_unbox = TRUE, pretty = TRUE))
