install.packages("RPostgres")
library(RPostgres)

#A - Calculate fee_dependence (regression mentioned in Assessment 1)
#connect with WRDS
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='s224770702')

#collect data from WRDS

data <- dbGetQuery(
  wrds,
  "SELECT
       company_fkey,
       auditor_fkey,
       fiscal_year::int   AS fiscal_year,
       audit_fees::numeric AS audit_fees
   FROM audit.feed03_audit_fees
   WHERE fiscal_year BETWEEN 2010 AND 2024"
)
head(data)
dbDisconnect(wrds)

install.packages("janitor")

library(dplyr)
library(janitor)

# Make fees from data just used by dbGetQuery()
#    (reason: make sure sum, max not error)
fees <- data %>%
  clean_names() %>%
  mutate(
    company_fkey = as.character(company_fkey),
    auditor_fkey = as.character(auditor_fkey),
    fiscal_year  = as.integer(fiscal_year),
    audit_fees   = as.numeric(audit_fees)
  )


#calculate fee_dependence at auditor level (audit_year)
#The original definition of fee-dependence is auditor–year: the proportion of 
revenue that auditor a receives from the largest client in year t

auditor_year_fd <- fees %>%
  group_by(auditor_fkey, fiscal_year) %>%
  summarise(
    max_client_fee = max(audit_fees, na.rm = TRUE),
    sum_audit_fees = sum(audit_fees, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    fee_dependence = if_else(
      sum_audit_fees > 0,
      max_client_fee / sum_audit_fees,
      NA_real_
    ),
    fee_dependence = pmin(pmax(fee_dependence, 0), 1)
  )

#ASSIGN fee_dependence to company–year (panel)
#After having fee_dependence, attach the same value to every company audited by 
auditor a in year t ⇒ each company–year row has a fee_dependence column.

# Merge auditor-year fee dependence into the company-year data
# This keeps all rows from 'fees' (left join)

panel <- merge(
  x     = fees,                        # left data frame: company-year level
  y     = auditor_year_fd,              # right data frame: auditor-year fee dependence
  by    = c("auditor_fkey", "fiscal_year"),  # keys to match on
  all.x = TRUE                          # TRUE means keep all rows from 'fees' (left join)
)

# Quick check of the result
head(panel)        # shows the first few rows
dim(panel)         # shows number of rows and columns

#B-Move to collect more variable and calculate "audit_quality" and attach in panel

# Data must to include for run OLS model
# fee_dependence (calculated above),
# closestfy_sum_audfees, closestfy_sum_audrel_fees, closestfy_sum_benfees,
# non_audit_fees, closestqu_incmst_netinc_ttm, closestqu_balsh_assets,
# audit_quality 


#reconnect WRDS
install.packages("dplyr")
library(RPostgres)
library(dplyr)
library(DBI)


if (!exists("wrds") || !DBI::dbIsValid(wrds)) {
  wrds <- dbConnect(
    Postgres(),
    host   = "wrds-pgdata.wharton.upenn.edu",
    port   = 9737,
    dbname = "wrds",
    sslmode = "require",
    user   = "s224770702"
  )
}
  
wrds_vars <- DBI::dbGetQuery(wrds, "
SELECT
  company_fkey,
  fiscal_year::int                      AS fiscal_year,
  closestfy_sum_audfees::numeric        AS closestfy_sum_audfees,
  non_audit_fees::numeric               AS non_audit_fees,
  closestfy_sum_benfees::numeric        AS closestfy_sum_benfees,
  closestqu_balsh_assets::numeric       AS closestqu_balsh_assets,
  closestqu_incmst_netinc_ttm::numeric  AS closestqu_incmst_netinc_ttm
FROM audit.feed03_audit_fees
WHERE fiscal_year BETWEEN 2010 AND 2024
ORDER BY company_fkey, fiscal_year
") |>
  janitor::clean_names() |>
  dplyr::mutate(
    company_fkey = as.character(company_fkey),
    fiscal_year  = as.integer(fiscal_year)
  )

## ---- Combine & create audit_quality ----
panel2 <- panel %>%
  mutate(company_fkey = as.character(company_fkey),
         fiscal_year  = as.integer(fiscal_year))

wrds2 <- wrds_vars %>%
  mutate(company_fkey = as.character(company_fkey),
         fiscal_year  = as.integer(fiscal_year))

panel_full <- panel2 %>%
  left_join(wrds2, by = c("company_fkey","fiscal_year")) %>% 
  mutate(
    log_firm_size = log(pmax(closestqu_balsh_assets, 1, na.rm = TRUE)),
    total_fees    = coalesce(closestfy_sum_audfees, 0) +
      coalesce(non_audit_fees, 0) +
      coalesce(closestfy_sum_benfees, 0),
    audit_quality = if_else(total_fees > 0,
                            closestfy_sum_audfees / total_fees,
                            NA_real_)
  )


## ---- Inspect ----
glimpse(panel_full)
head(select(panel_full, company_fkey, fiscal_year, fee_dependence, total_fees, audit_quality))

#C Run the OLS regression
#---------------------------------------------
# Ordinary Least Squares (OLS) regression
# Dependent variable: audit_quality
# Explanatory variables: fee_dependence, log_firm_size,
#                        non_audit_fees, closestqu_incmst_netinc_ttm
#---------------------------------------------

library(dplyr)
library(tidyr)

panel_ols <- panel_full %>%
  select(audit_quality, fee_dependence, log_firm_size,
         non_audit_fees, closestqu_incmst_netinc_ttm) %>%
  drop_na() 

model_ols <- lm(
  audit_quality ~ fee_dependence + log_firm_size +
    non_audit_fees + closestqu_incmst_netinc_ttm,
  data = panel_ols
)

summary(model_ols)

install.packages("sandwich")
install.packages("modelsummary")  
library(modelsummary)
library(sandwich)

# table SE robust
msummary(
  model_ols,
  vcov = "HC1",
  statistic = "({std.error})",     # hiện SE trong ngoặc
  gof_omit = "IC|Log|Adj|AIC|BIC"  # ẩn bớt chỉ số không cần
)

# Export Word / HTML / LaTeX
modelsummary(model_ols, vcov="HC1", output = "model_ols.docx")  # Word
modelsummary(model_ols, vcov="HC1", output = "model_ols.html")  # HTML

#D Relationship between fee_dependence & audit_quality
library(dplyr)
library(ggplot2)
library(scales)

df <- panel_full %>%
  select(fee_dependence, audit_quality, fiscal_year) %>%
  filter(!is.na(fee_dependence), !is.na(audit_quality)) %>%
  filter(between(fee_dependence, 0, 1), between(audit_quality, 0, 1))

set.seed(1)
df_small <- if (nrow(df) > 30000) dplyr::slice_sample(df, n = 30000) else df

#Relationship
library(ggplot2); library(scales)
df <- panel_full |> dplyr::select(fee_dependence, audit_quality) |>
  tidyr::drop_na() |> dplyr::filter(dplyr::between(fee_dependence,0,1),
                                    dplyr::between(audit_quality,0,1))
ggplot(df, aes(fee_dependence, audit_quality)) +
  geom_point(alpha=.15, size=.6) +
  geom_smooth(method="lm", se=TRUE) +
  scale_x_continuous(labels=label_percent(1)) +
  scale_y_continuous(labels=label_percent(1)) +
  labs(title="Fee Dependence vs Audit Quality",
       x="Fee dependence", y="Audit quality") +
  theme_minimal()

#E Time-series
install.packages("slider")
library(dplyr)
library(slider)   
library(ggplot2)
library(scales)

annual <- panel_full %>%
  filter(fiscal_year >= 2010) %>%             
  group_by(fiscal_year) %>%
  summarise(
    audit_quality               = mean(audit_quality, na.rm = TRUE),
    fee_dependence              = mean(fee_dependence, na.rm = TRUE),
    log_firm_size               = mean(log_firm_size,  na.rm = TRUE),
    non_audit_fees              = mean(non_audit_fees, na.rm = TRUE),
    closestqu_incmst_netinc_ttm = mean(closestqu_incmst_netinc_ttm, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(fiscal_year)

#estimate β (of fee_dependence)
estimate_beta_fd <- function(data, min_obs = 3, with_controls = TRUE) {
  if (nrow(data) < min_obs || sd(data$fee_dependence, na.rm = TRUE) == 0) return(NA_real_)
  fml <- if (with_controls) {
    audit_quality ~ fee_dependence + log_firm_size +
      non_audit_fees + closestqu_incmst_netinc_ttm
  } else {
    audit_quality ~ fee_dependence
  }
  fit <- lm(fml, data = data)
  unname(coef(fit)["fee_dependence"])
}

#Rolling β
k <- 5  

annual$beta_fd <- slide_index_dbl(
  .x        = annual,
  .i        = annual$fiscal_year,     
  .before   = k - 1,                 
  .complete = TRUE,                   
  .f        = ~ estimate_beta_fd(.x, min_obs = 3, with_controls = TRUE)
)

#Draw image with rolling follow by year
ggplot(filter(annual, !is.na(beta_fd)), aes(fiscal_year, beta_fd)) +
  geom_line() + geom_point() +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = paste0("Rolling ", k, "-year β of fee_dependence on audit_quality (full sample)"),
       x = "Fiscal year", y = "β_fd (rolling)") +
  theme_minimal()

