-- 1. CLEANUP (DROP TABLES)
-- We drop in reverse order of dependencies to avoid foreign key errors
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS draft CASCADE;
DROP TABLE IF EXISTS plays CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS schedule CASCADE;
DROP TABLE IF EXISTS rosters CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

-- 2. TEAMS TABLE
CREATE TABLE teams (
    id BIGINT PRIMARY KEY,
    fullName TEXT,
    teamAbbrev TEXT UNIQUE,
    teamCommonName TEXT,
    teamPlaceName TEXT,
    activeStatus BOOLEAN, -- API: activeStatus
    firstSeasonId BIGINT,
    lastSeasonId BIGINT,
    mostRecentTeamId BIGINT,
    conferenceName TEXT,
    divisionName TEXT,
    franchiseId BIGINT,
    logos TEXT,
    scrapedOn TIMESTAMPTZ DEFAULT now()
);

-- 3. PLAYERS TABLE
CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    firstName_default TEXT, -- API: firstName.default (dot becomes underscore)
    lastName_default TEXT,
    headshot TEXT,
    positionCode TEXT,
    shootsCatches TEXT,
    heightInInches NUMERIC,
    heightInCentimeters NUMERIC,
    weightInPounds NUMERIC,
    weightInKilograms NUMERIC,
    birthDate DATE,
    birthCountry TEXT,
    birthCity_default TEXT,
    birthStateProvince_default TEXT,
    scrapedOn TIMESTAMPTZ DEFAULT now()
);

-- 4. ROSTERS TABLE
CREATE TABLE rosters (
    id BIGINT REFERENCES players(id),
    season BIGINT NOT NULL,
    teamAbbrev TEXT REFERENCES teams(teamAbbrev),
    sweaterNumber NUMERIC,
    positionCode TEXT,
    scrapedOn TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (id, season)
);

-- 5. SCHEDULE TABLE
CREATE TABLE schedule (
    id BIGINT PRIMARY KEY,
    season BIGINT,
    gameDate DATE,
    gameType NUMERIC,
    gameState TEXT, -- API: gameState
    homeTeam_id BIGINT,
    homeTeam_abbrev TEXT,
    homeTeam_score NUMERIC,
    homeTeam_commonName_default TEXT,
    homeTeam_placeName_default TEXT,
    homeTeam_logo TEXT,
    awayTeam_id BIGINT,
    awayTeam_abbrev TEXT,
    awayTeam_score NUMERIC,
    awayTeam_commonName_default TEXT,
    awayTeam_placeName_default TEXT,
    awayTeam_logo TEXT,
    venue_default TEXT,
    venue_location_default TEXT,
    startTimeUTC TIMESTAMPTZ,
    easternUtcOffset TEXT,
    venueUtcOffset TEXT,
    gameCenterLink TEXT,
    scrapedOn TIMESTAMPTZ DEFAULT now()
);

-- 6. PLAYER_STATS TABLE
CREATE TABLE player_stats (
    id TEXT PRIMARY KEY, -- playerid_season_strength
    playerId BIGINT REFERENCES players(id),
    season BIGINT NOT NULL,
    is_goalie BOOLEAN,
    team TEXT,
    opp TEXT,
    strength TEXT,
    gamesPlayed NUMERIC,
    gamesStarted NUMERIC,
    goals NUMERIC,
    assists NUMERIC,
    points NUMERIC,
    plusMinus NUMERIC,
    penaltyMinutes NUMERIC,
    powerPlayGoals NUMERIC,
    shortHandedGoals NUMERIC,
    gameWinningGoals NUMERIC,
    overTimeGoals NUMERIC,
    shots NUMERIC,
    shotsAgainst NUMERIC,
    saves NUMERIC,
    goalsAgainst NUMERIC,
    shutouts NUMERIC,
    shootingPctg NUMERIC,
    savePercentage NUMERIC,
    goalsAgainstAverage NUMERIC,
    cf NUMERIC, ca NUMERIC, cf_pct NUMERIC,
    ff NUMERIC, fa NUMERIC, ff_pct NUMERIC,
    sf NUMERIC, sa NUMERIC, sf_pct NUMERIC,
    gf NUMERIC, ga NUMERIC, gf_pct NUMERIC,
    xg NUMERIC, xga NUMERIC, xgf_pct NUMERIC,
    pf NUMERIC, pa NUMERIC,
    give_for NUMERIC, give_against NUMERIC,
    take_for NUMERIC, take_against NUMERIC,
    seconds NUMERIC,
    minutes NUMERIC,
    avgTimeOnIcePerGame NUMERIC,
    avgShiftsPerGame NUMERIC,
    faceoffWinPctg NUMERIC,
    scrapedOn TIMESTAMPTZ DEFAULT now()
);

-- 7. PLAYS TABLE
CREATE TABLE plays (
    id TEXT PRIMARY KEY,
    game_id BIGINT REFERENCES schedule(id),
    event_id BIGINT,
    period NUMERIC,
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
    scrapedOn TIMESTAMPTZ DEFAULT now()
);

-- 8. DRAFT TABLE
CREATE TABLE draft (
    year BIGINT,
    overallPick BIGINT,
    roundNumber BIGINT,
    pickInRound BIGINT,
    team_triCode TEXT,
    player_id BIGINT,
    player_firstName TEXT,
    player_lastName TEXT,
    player_position TEXT,
    player_birthCountry TEXT,
    player_birthStateProvince TEXT,
    player_years_pro NUMERIC,
    amateurClubName TEXT,
    amateurLeague TEXT,
    countryCode TEXT,
    displayAbbrev_default TEXT,
    scrapedOn TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (year, overallPick)
);

-- 9. STANDINGS TABLE
CREATE TABLE standings (
    date DATE,
    teamAbbrev_default TEXT,
    teamName_default TEXT,
    teamCommonName_default TEXT,
    conferenceName TEXT,
    divisionName TEXT,
    gamesPlayed NUMERIC,
    wins NUMERIC,
    losses NUMERIC,
    otLosses NUMERIC,
    points NUMERIC,
    pointPctg NUMERIC,
    regulationWins NUMERIC,
    row NUMERIC,
    goalsFor NUMERIC,
    goalsAgainst NUMERIC,
    goalDifferential NUMERIC,
    streakCode TEXT,
    streakCount NUMERIC,
    scrapedOn TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date, teamAbbrev_default)
);

-- Reload PostgREST to recognize changes
NOTIFY pgrst, 'reload schema';