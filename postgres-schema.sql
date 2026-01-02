-- 1. CLEANUP
DROP TABLE IF EXISTS draft CASCADE;
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS plays CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS schedule CASCADE;
DROP TABLE IF EXISTS rosters CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

-- 2. TEAMS
CREATE TABLE teams (
    id BIGINT PRIMARY KEY, abbrev TEXT, seasonid BIGINT, logo TEXT, darklogo TEXT,
    french BOOLEAN, scrapedon TIMESTAMPTZ, source TEXT, commonname_default TEXT,
    name_default TEXT, name_fr TEXT, placenamewithpreposition_default TEXT,
    placenamewithpreposition_fr TEXT, placename_default TEXT, placename_fr TEXT, commonname_fr TEXT
);

-- 3. PLAYERS
CREATE TABLE players (
    id BIGINT PRIMARY KEY, headshot TEXT, sweaternumber NUMERIC, positioncode TEXT,
    shootscatches TEXT, heightininches NUMERIC, weightinpounds NUMERIC, heightincentimeters NUMERIC,
    weightinkilograms NUMERIC, birthdate DATE, birthcountry TEXT, firstname_default TEXT,
    lastname_default TEXT, birthcity_default TEXT, birthstateprovince_default TEXT,
    source TEXT, scrapedon TIMESTAMPTZ, season BIGINT, teamabbrev TEXT,
    birthcity_sv TEXT, birthcity_fi TEXT, birthcity_cs TEXT, birthcity_sk TEXT, birthcity_fr TEXT,
    firstname_sv TEXT, firstname_de TEXT, firstname_sk TEXT, firstname_cs TEXT, firstname_es TEXT, firstname_fi TEXT
);

-- 4. ROSTERS
CREATE TABLE rosters (
    id BIGINT REFERENCES players(id), season BIGINT, teamabbrev TEXT,
    sweaternumber NUMERIC, positioncode TEXT, firstname_default TEXT, lastname_default TEXT,
    -- Intl columns required for bulk upsert compatibility
    birthcity_sv TEXT, birthcity_fi TEXT, birthcity_cs TEXT, birthcity_sk TEXT, birthcity_fr TEXT,
    firstname_sv TEXT, firstname_de TEXT, firstname_sk TEXT, firstname_cs TEXT, firstname_es TEXT, firstname_fi TEXT,
    birthdate DATE, birthcountry TEXT, birthcity_default TEXT, birthstateprovince_default TEXT,
    headshot TEXT, weightinpounds NUMERIC, heightininches NUMERIC, scrapedon TIMESTAMPTZ,
    PRIMARY KEY (id, season)
);

-- 5. SCHEDULE
CREATE TABLE schedule (
    id BIGINT PRIMARY KEY, season BIGINT, gametype NUMERIC, gamedate DATE, neutralsite BOOLEAN,
    starttimeutc TEXT, gamestate TEXT, gamecenterlink TEXT, venue_default TEXT,
    awayteam_id BIGINT, awayteam_abbrev TEXT, awayteam_score NUMERIC,
    hometeam_id BIGINT, hometeam_abbrev TEXT, hometeam_score NUMERIC,
    tvbroadcasts JSONB, source TEXT, scrapedon TIMESTAMPTZ,
    -- Missing columns from logs
    awayteam_darklogo TEXT, awayteam_logo TEXT, hometeam_darklogo TEXT, hometeam_logo TEXT,
    awayteam_commonname_default TEXT, hometeam_commonname_default TEXT,
    awayteam_placename_default TEXT, hometeam_placename_default TEXT,
    awayteam_airlinelink TEXT, hometeam_airlinelink TEXT,
    awayteam_airlinedesc TEXT, hometeam_airlinedesc TEXT,
    awayteam_hotellink TEXT, hometeam_hotellink TEXT,
    awayteam_hoteldesc TEXT, hometeam_hoteldesc TEXT,
    gameschedulestate TEXT, venuetimezone TEXT, venueutcoffset TEXT, easternutcoffset TEXT,
    perioddescriptor_periodtype TEXT, perioddescriptor_maxregulationperiods NUMERIC,
    gameoutcome_lastperiodtype TEXT, threeminrecap TEXT, threeminrecapfr TEXT,
    condensedgame TEXT, condensedgamefr TEXT, winninggoalie_playerid NUMERIC,
    winninggoalie_lastname_default TEXT, winninggoalie_firstinitial_default TEXT,
    winninggoalie_lastname_sk TEXT, winninggoalie_lastname_fi TEXT, winninggoalie_lastname_cs TEXT, winninggoalie_lastname_sv TEXT,
    winninggoalscorer_playerid NUMERIC, winninggoalscorer_lastname_default TEXT, winninggoalscorer_firstinitial_default TEXT,
    winninggoalscorer_lastname_sk TEXT, winninggoalscorer_lastname_fi TEXT, winninggoalscorer_lastname_cs TEXT,
    venue_fr TEXT, venue_es TEXT, hometeam_commonname_fr TEXT, awayteam_commonname_fr TEXT,
    hometeam_placename_fr TEXT, awayteam_placename_fr TEXT,
    hometeam_placenamewithpreposition_fr TEXT, awayteam_placenamewithpreposition_fr TEXT,
    hometeam_placenamewithpreposition_default TEXT, awayteam_placenamewithpreposition_default TEXT,
    hometeam_radiolink TEXT, awayteam_radiolink TEXT, hometeam_promolink TEXT, awayteam_promolink TEXT,
    hometeam_promodesc TEXT, awayteam_promodesc TEXT, hometeam_homesplitsquad BOOLEAN, awayteam_awaysplitsquad BOOLEAN
);

-- 6. PLAYS (Operational + JSONB Catch-all)
CREATE TABLE plays (
    id TEXT PRIMARY KEY, gameid BIGINT REFERENCES schedule(id), sortorder NUMERIC,
    per NUMERIC, strength TEXT, event TEXT, description TEXT, time TEXT,
    timeremaining TEXT, eventteam TEXT, player1id BIGINT, player1name TEXT,
    xg NUMERIC, scrapedon TIMESTAMPTZ,
    raw_data JSONB -- Catch-all for the 100+ missing columns
);

-- 7. PLAYER_STATS
CREATE TABLE player_stats (
    id TEXT PRIMARY KEY, player1id BIGINT REFERENCES players(id), player1name TEXT,
    eventteam TEXT, strength TEXT, season BIGINT, seconds NUMERIC, minutes NUMERIC,
    goals NUMERIC, assists NUMERIC, points NUMERIC, shots NUMERIC,
    cf NUMERIC, ca NUMERIC, ff NUMERIC, fa NUMERIC, sf NUMERIC, sa NUMERIC,
    gf NUMERIC, ga NUMERIC, xg NUMERIC, xga NUMERIC, pf NUMERIC, pa NUMERIC,
    gamesplayed NUMERIC, scrapedon TIMESTAMPTZ DEFAULT now()
);

-- 8. STANDINGS
CREATE TABLE standings (
    id TEXT PRIMARY KEY, date DATE, seasonid BIGINT, conferencename TEXT, divisionname TEXT,
    gamesplayed NUMERIC, wins NUMERIC, losses NUMERIC, otlosses NUMERIC, points NUMERIC,
    pointpctg NUMERIC, goaldifferential NUMERIC, streakcode TEXT, streakcount NUMERIC,
    teamabbrev_default TEXT, teamname_default TEXT, scrapedon TIMESTAMPTZ,
    conferenceabbrev TEXT, conferencehomesequence NUMERIC, conferencesequence NUMERIC, conferencel10sequence NUMERIC, conferenceroadsequence NUMERIC,
    divisionabbrev TEXT, divisionhomesequence NUMERIC, divisionsequence NUMERIC, divisionl10sequence NUMERIC, divisionroadsequence NUMERIC,
    leaguehomesequence NUMERIC, leaguesequence NUMERIC, leaguel10sequence NUMERIC, leagueroadsequence NUMERIC,
    homepoints NUMERIC, homewins NUMERIC, homelosses NUMERIC, homeotlosses NUMERIC, hometies NUMERIC,
    homegoalsfor NUMERIC, homegoalsagainst NUMERIC, homegoaldifferential NUMERIC, homeregulationwins NUMERIC, homeregulationplusotwins NUMERIC, homegamesplayed NUMERIC,
    roadpoints NUMERIC, roadwins NUMERIC, roadlosses NUMERIC, roadotlosses NUMERIC, roadties NUMERIC,
    roadgoalsfor NUMERIC, roadgoalsagainst NUMERIC, roadgoaldifferential NUMERIC, roadregulationwins NUMERIC, roadregulationplusotwins NUMERIC, roadgamesplayed NUMERIC,
    l10points NUMERIC, l10wins NUMERIC, l10losses NUMERIC, l10otlosses NUMERIC, l10ties NUMERIC,
    l10goalsfor NUMERIC, l10goalsagainst NUMERIC, l10goaldifferential NUMERIC, l10regulationwins NUMERIC, l10regulationplusotwins NUMERIC, l10gamesplayed NUMERIC,
    gametypeid NUMERIC, goalfor NUMERIC, goalagainst NUMERIC, shootoutwins NUMERIC, shootoutlosses NUMERIC,
    winpctg NUMERIC, goalsforpctg NUMERIC, goaldifferentialpctg NUMERIC, ties NUMERIC,
    regulationplusotwinpctg NUMERIC, regulationplusotwins NUMERIC, regulationwinpctg NUMERIC, regulationwins NUMERIC,
    wildcardsequence NUMERIC, waiverssequence NUMERIC, teamlogo TEXT, source TEXT,
    placename_default TEXT, placename_fr TEXT, teamname_fr TEXT, teamcommonname_default TEXT, teamcommonname_fr TEXT
);