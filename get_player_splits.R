### retrieve splits xwOBA data for hitters and pitchers

library(tidyverse)
library(rvest) # need for read_html() in get_active_player_ids

'%ni%' <- Negate('%in%') # always a good function to have

# load function for getting active player id, name, team, and position info
get_active_player_ids <- function() {
  # first read the html from Razzball's player id page
  player_id_page <- read_html("https://razzball.com/mlbamids/")
  
  raw_table <- html_table(player_id_page)[[2]]
  
  player_id_table <-
    raw_table %>%
    select(Name, MLBAMID, Team, ESPN_POS) %>%
    rename(name = Name,
           team = Team,
           position = ESPN_POS)
  
  player_id_table
}

## function to load data from the SQLite database
retrieve_pitch_data <- function(database_name, todays_date = NULL) {
  db <- dbConnect(SQLite(), dbname = database_name)
  dbname <- dbListTables(db)
  ## let's load all the data into a data frame/tibble
  if (is_empty(todays_date)) {
    min_year = 2018 # if no date is given, just use all Statcast data that we have
  } else{
    min_year = year(todays_date) - 4
  }
  min_date <- str_c("'", min_year, "-01-01", "'", sep = "")
  integer_date <- interval("1970-01-01", min_date)/duration(1, units = "days")
  query = str_glue("SELECT * FROM ", dbname, " WHERE game_date > ", integer_date, sep = " ")
  pitch_data <- as_tibble(dbGetQuery(db, query)) # store all of our data in a tibble for ease of use
  dbDisconnect(db) # close connection to database
  rm(db)
  
  ## change game_date to yyyy-mm-dd format
  pitch_data <- pitch_data %>% mutate(game_date = as.Date(game_date, origin = "1970-01-01"))
  
  pitch_data
}

## filter down the pitch data in to a table that includes only PAs with a defined
## xwOBA
get_pa_data <- function(data_by_pitch, min_date, max_date) {
  if (missing(min_date)) {
    min_date <-
      data_by_pitch %>%
      pull(game_date) %>%
      min()
  }
  if (missing(max_date)) {
    max_date <-
      data_by_pitch %>%
      pull(game_date) %>%
      max()
  }
  
  pa_data <- data_by_pitch %>%
    filter(game_date >= min_date,
           game_date <= max_date,
           woba_denom == 1) %>%
    select(game_pk, game_date, player_name, batter, pitcher, events, description, bb_type, stand, p_throws, home_team, away_team, inning_topbot, launch_speed, launch_angle, estimated_woba_using_speedangle, woba_value)
  pa_data
}

## aggregate the batters' stats
get_batter_stats <- function(data_by_pa) {
  # with this function, we want to group the PA data by batter ID and pitcher-
  # handedness to get the xwOBA for those PAs
  batter_stats <-
    data_by_pa %>%
    filter(!is.na(estimated_woba_using_speedangle) | events %in% c("hit_by_pitch", "strikeout", "strikeout_double_play", "walk")) %>%
    mutate(xwOBA_pa = case_when(!is.na(estimated_woba_using_speedangle) ~ estimated_woba_using_speedangle,
                                events == "walk" ~ 0.693,
                                events == "hit_by_pitch" ~ 0.724,
                                events %in% c("strikeout", "strikeout_double_play") ~ 0,
                                TRUE ~ 0)) %>%
    group_by(batter, stand, p_throws) %>%
    summarise(
      pa = n(),
      xwOBA = mean(xwOBA_pa)) %>%
    ungroup() %>%
    rename(batter_id = batter, batter_hand = stand)
  
  batter_stats
}

## aggregate the pitchers' stats
get_pitcher_stats <- function(data_by_pa) {
  # with this function, we want to group the PA data by batter ID and pitcher-
  # handedness to get the xwOBA for those PAs
  pitcher_stats <-
    data_by_pa %>%
    filter(!is.na(estimated_woba_using_speedangle) | events %in% c("hit_by_pitch", "strikeout", "strikeout_double_play", "walk")) %>%
    mutate(xwOBA_pa = case_when(!is.na(estimated_woba_using_speedangle) ~ estimated_woba_using_speedangle,
                                events == "walk" ~ 0.693,
                                events == "hit_by_pitch" ~ 0.724,
                                events %in% c("strikeout", "strikeout_double_play") ~ 0,
                                TRUE ~ 0)) %>%
    group_by(pitcher, p_throws, stand) %>%
    summarise(
      pa = n(),
      xwOBA = mean(xwOBA_pa)) %>%
    ungroup() %>%
    rename(pitcher_id = pitcher, batter_hand = stand)
  
  pitcher_stats
}

db_path <- "~/statcast_data/statcast_2022.sqlite"
pitch_data <- retrieve_pitch_data(db_path,
                    todays_date = today())

pa_data <-
  pitch_data %>%
  get_pa_data()

batter_stats <-
  pa_data %>%
  get_batter_stats()

player_id_map <- get_active_player_ids()

id <- 663656 # this will allow you to view Kyle Tucker's splits

batter_stats %>%
  filter(batter_id == 663656) %>%
  left_join(player_id_map, by = c("batter_id" = "MLBAMID")) %>%
  select(name, batter_id, team, position, batter_hand, p_throws, pa, xwOBA)

