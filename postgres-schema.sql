-- 1. CLEANUP
DROP TABLE IF EXISTS draft CASCADE;
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS plays CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS team_stats CASCADE;
DROP TABLE IF EXISTS schedule CASCADE;
DROP TABLE IF EXISTS rosters CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

-- 2. TEAMS (Literal Match to teams.csv)
CREATE TABLE teams (
    id BIGINT PRIMARY KEY,
    seasonid BIGINT,
    abbrev TEXT,
    logo TEXT,
    darklogo TEXT,
    french BOOLEAN,
    scrapedon TIMESTAMPTZ,
    source TEXT,
    commonname_default TEXT,
    name_default TEXT,
    name_fr TEXT,
    placenamewithpreposition_default TEXT,
    placenamewithpreposition_fr TEXT,
    placename_default TEXT,
    placename_fr TEXT,
    commonname_fr TEXT
);

-- 3. PLAYERS (Literal Match to roster.csv)
CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    headshot TEXT,
    sweaternumber NUMERIC,
    positioncode TEXT,
    shootscatches TEXT,
    heightininches NUMERIC,
    weightinpounds NUMERIC,
    heightincentimeters NUMERIC,
    weightinkilograms NUMERIC,
    birthdate DATE,
    birthcountry TEXT,
    scrapedon TIMESTAMPTZ,
    source TEXT,
    firstname_default TEXT,
    lastname_default TEXT,
    birthcity_default TEXT,
    birthstateprovince_default TEXT,
    birthcity_fi TEXT, birthcity_sv TEXT, birthcity_cs TEXT, birthcity_sk TEXT,
    firstname_cs TEXT, firstname_de TEXT, firstname_es TEXT, firstname_fi TEXT, 
    firstname_sk TEXT, firstname_sv TEXT, birthcity_fr TEXT
);

-- 4. ROSTERS (Source: roster.py)
CREATE TABLE rosters (
    id BIGINT REFERENCES players(id),
    season BIGINT,
    teamabbrev TEXT,
    sweaternumber NUMERIC,
    positioncode TEXT,
    PRIMARY KEY (id, season)
);

-- 5. SCHEDULE (Literal Match to schedule.csv)
CREATE TABLE schedule (
    id BIGINT PRIMARY KEY,
    season BIGINT,
    gametype NUMERIC,
    gamedate DATE,
    neutralsite BOOLEAN,
    starttimeutc TEXT,
    easternutcoffset TEXT,
    venueutcoffset TEXT,
    venuetimezone TEXT,
    gamestate TEXT,
    gameschedulestate TEXT,
    tvbroadcasts JSONB,
    gamecenterlink TEXT,
    scrapedon TIMESTAMPTZ,
    source TEXT,
    venue_default TEXT,
    awayteam_id BIGINT,
    awayteam_abbrev TEXT,
    awayteam_score NUMERIC,
    hometeam_id BIGINT,
    hometeam_abbrev TEXT,
    hometeam_score NUMERIC
);

-- 6. PLAYS (Literal Match to game.csv / Plays)
CREATE TABLE plays (
    id TEXT PRIMARY KEY, -- gameid_sortorder
    gameid BIGINT REFERENCES schedule(id),
    per NUMERIC,
    strength TEXT,
    event TEXT,
    description TEXT,
    time TEXT,
    timeremaining TEXT,
    xcoord NUMERIC,
    ycoord NUMERIC,
    zonecode TEXT,
    eventteam TEXT,
    player1id BIGINT,
    player2id BIGINT,
    player3id BIGINT,
    xg NUMERIC,
    scrapedon TIMESTAMPTZ
);

-- 7. PLAYER_STATS (Aggregated Analytics)
CREATE TABLE player_stats (
    id TEXT PRIMARY KEY, -- player1id_season_strength
    player1id BIGINT REFERENCES players(id),
    player1name TEXT,
    eventteam TEXT,
    strength TEXT,
    season BIGINT,
    seconds NUMERIC,
    minutes NUMERIC,
    goals NUMERIC,
    assists NUMERIC,
    points NUMERIC,
    shots NUMERIC,
    cf NUMERIC, ca NUMERIC,
    ff NUMERIC, fa NUMERIC,
    sf NUMERIC, sa NUMERIC,
    gf NUMERIC, ga NUMERIC,
    xg NUMERIC, xga NUMERIC,
    gamesplayed NUMERIC,
    scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 8. STANDINGS (Literal Match to standings.csv)
CREATE TABLE standings (
    id TEXT PRIMARY KEY,
    date DATE,
    seasonid BIGINT,
    conferencename TEXT,
    divisionname TEXT,
    gamesplayed NUMERIC,
    wins NUMERIC,
    losses NUMERIC,
    otlosses NUMERIC,
    points NUMERIC,
    pointpctg NUMERIC,
    goaldifferential NUMERIC,
    streakcode TEXT,
    streakcount NUMERIC,
    teamabbrev_default TEXT,
    scrapedon TIMESTAMPTZ
);

-- 9. DRAFT (Literal Match to draft.csv)
CREATE TABLE draft (
    id TEXT PRIMARY KEY,
    year BIGINT,
    overallpick BIGINT,
    round NUMERIC,
    pickinround NUMERIC,
    teamabbrev TEXT,
    firstname_default TEXT,
    lastname_default TEXT,
    positioncode TEXT,
    amateurleague TEXT,
    amateurclubname TEXT,
    scrapedon TIMESTAMPTZ
);