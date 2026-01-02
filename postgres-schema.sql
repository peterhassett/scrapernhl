DROP TABLE IF EXISTS plays CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS schedule CASCADE;
DROP TABLE IF EXISTS rosters CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS draft CASCADE;

-- 1. TEAMS
CREATE TABLE teams (
    id BIGINT PRIMARY KEY,
    fullname TEXT,
    teamabbrev TEXT UNIQUE,
    teamcommonname TEXT,
    teamplacename TEXT,
    firstseasonid BIGINT,
    lastseasonid BIGINT,
    mostrecentteamid BIGINT,
    active_status BOOLEAN,
    conference_name TEXT,
    division_name TEXT,
    franchiseid BIGINT,
    logos TEXT,
    scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 2. PLAYERS
CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    firstname_default TEXT,
    lastname_default TEXT,
    headshot TEXT,
    positioncode TEXT,
    shootscatches TEXT,
    heightininches BIGINT,
    heightincentimeters BIGINT,
    weightinpounds BIGINT,
    weightinkilograms BIGINT,
    birthdate DATE,
    birthcountry TEXT,
    birthcity_default TEXT,
    birthstateprovince_default TEXT,
    scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 3. ROSTERS
CREATE TABLE rosters (
    id BIGINT REFERENCES players(id),
    season BIGINT NOT NULL,
    teamabbrev TEXT REFERENCES teams(teamabbrev),
    sweaternumber BIGINT,
    positioncode TEXT,
    scrapedon TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (id, season)
);

-- 4. PLAYER_STATS
CREATE TABLE player_stats (
    id TEXT PRIMARY KEY,
    playerid BIGINT REFERENCES players(id),
    season BIGINT NOT NULL,
    is_goalie BOOLEAN,
    team TEXT,
    opp TEXT,
    strength TEXT,
    gamesplayed BIGINT,
    gamesstarted BIGINT,
    goals BIGINT,
    assists BIGINT,
    points BIGINT,
    plusminus BIGINT,
    penaltyminutes BIGINT,
    powerplaygoals BIGINT,
    shorthandedgoals BIGINT,
    gamewinninggoals BIGINT,
    overtimegoals BIGINT,
    shots BIGINT,
    shotsagainst BIGINT,
    saves BIGINT,
    goalsagainst BIGINT,
    shutouts BIGINT,
    shootingpctg NUMERIC,
    savepercentage NUMERIC,
    goalsagainstaverage NUMERIC,
    cf BIGINT, ca BIGINT, cf_pct NUMERIC,
    ff BIGINT, fa BIGINT, ff_pct NUMERIC,
    sf BIGINT, sa BIGINT, sf_pct NUMERIC,
    gf BIGINT, ga BIGINT, gf_pct NUMERIC,
    xg NUMERIC, xga NUMERIC, xgf_pct NUMERIC,
    pf BIGINT, pa BIGINT,
    give_for BIGINT, give_against BIGINT,
    take_for BIGINT, take_against BIGINT,
    seconds NUMERIC,
    minutes NUMERIC,
    avgtimeonicepergame NUMERIC,
    avgshiftspergame NUMERIC,
    faceoffwinpctg NUMERIC,
    scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 5. SCHEDULE
CREATE TABLE schedule (
    id BIGINT PRIMARY KEY,
    season BIGINT,
    gamedate DATE,
    gametype BIGINT,
    gamestate TEXT,
    hometeam_id BIGINT,
    hometeam_abbrev TEXT,
    hometeam_score BIGINT,
    hometeam_commonname_default TEXT,
    hometeam_placename_default TEXT,
    hometeam_logo TEXT,
    awayteam_id BIGINT,
    awayteam_abbrev TEXT,
    awayteam_score BIGINT,
    awayteam_commonname_default TEXT,
    awayteam_placename_default TEXT,
    awayteam_logo TEXT,
    venue_default TEXT,
    venue_location_default TEXT,
    starttimeutc TIMESTAMPTZ,
    easternutcoffset TEXT,
    venueutcoffset TEXT,
    gamecenterlink TEXT,
    scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 6. PLAYS
CREATE TABLE plays (
    id TEXT PRIMARY KEY,
    game_id BIGINT REFERENCES schedule(id),
    event_id BIGINT,
    period BIGINT,
    period_type TEXT,
    time_in_period TEXT,
    time_remaining TEXT,
    situation_code TEXT,
    home_team_defending_side TEXT,
    event_type TEXT,
    type_desc_key TEXT,
    x_coord NUMERIC,
    y_coord NUMERIC,
    zone_code TEXT,
    ppt_replay_url TEXT,
    scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 7. DRAFT
CREATE TABLE draft (
    year BIGINT,
    overall_pick BIGINT,
    round_number BIGINT,
    pick_in_round BIGINT,
    team_tricode TEXT,
    player_id BIGINT,
    player_firstname TEXT,
    player_lastname TEXT,
    player_position TEXT,
    player_birthcountry TEXT,
    player_birthstateprovince TEXT,
    player_years_pro BIGINT,
    amateurclubname TEXT,
    amateurleague TEXT,
    countrycode TEXT,
    displayabbrev_default TEXT,
    scrapedon TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (year, overall_pick)
);

-- 8. STANDINGS
CREATE TABLE standings (
    date DATE,
    teamabbrev_default TEXT,
    teamname_default TEXT,
    teamcommonname_default TEXT,
    conference_name TEXT,
    division_name TEXT,
    gamesplayed BIGINT,
    wins BIGINT,
    losses BIGINT,
    otlosses BIGINT,
    points BIGINT,
    pointpctg NUMERIC,
    regulationwins BIGINT,
    row BIGINT,
    goalsfor BIGINT,
    goalsagainst BIGINT,
    goaldifferential BIGINT,
    streak_code TEXT,
    streak_count BIGINT,
    scrapedon TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date, teamabbrev_default)
);

NOTIFY pgrst, 'reload schema';