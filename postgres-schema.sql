-- 1. CLEANUP
DROP TABLE IF EXISTS draft CASCADE;
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS plays CASCADE;
DROP TABLE IF EXISTS player_stats CASCADE;
DROP TABLE IF EXISTS schedule CASCADE;
DROP TABLE IF EXISTS rosters CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

-- 2. TEAMS (Literal Match to teams.csv)
CREATE TABLE teams (
    id BIGINT PRIMARY KEY, seasonid BIGINT, abbrev TEXT, logo TEXT, darklogo TEXT,
    french BOOLEAN, scrapedon TIMESTAMPTZ, source TEXT, commonname_default TEXT,
    name_default TEXT, name_fr TEXT, placenamewithpreposition_default TEXT,
    placenamewithpreposition_fr TEXT, placename_default TEXT, placename_fr TEXT, commonname_fr TEXT
);

-- 3. PLAYERS (Full Bio Support)
CREATE TABLE players (
    id BIGINT PRIMARY KEY, headshot TEXT, sweaternumber NUMERIC, positioncode TEXT,
    shootscatches TEXT, heightininches NUMERIC, weightinpounds NUMERIC, heightincentimeters NUMERIC,
    weightinkilograms NUMERIC, birthdate DATE, birthcountry TEXT, scrapedon TIMESTAMPTZ,
    source TEXT, firstname_default TEXT, lastname_default TEXT, birthcity_default TEXT,
    birthstateprovince_default TEXT, birthcity_fi TEXT, birthcity_sv TEXT, birthcity_cs TEXT,
    birthcity_sk TEXT, firstname_cs TEXT, firstname_de TEXT, firstname_es TEXT,
    firstname_fi TEXT, firstname_sk TEXT, firstname_sv TEXT, birthcity_fr TEXT,
    season BIGINT, teamabbrev TEXT
);

-- 4. ROSTERS
CREATE TABLE rosters (
    id BIGINT REFERENCES players(id), season BIGINT, teamabbrev TEXT,
    sweaternumber NUMERIC, positioncode TEXT, firstname_default TEXT, lastname_default TEXT,
    birthcity_sv TEXT, birthcity_fi TEXT, birthcity_cs TEXT, birthcity_sk TEXT, birthcity_fr TEXT,
    firstname_sv TEXT, firstname_de TEXT, firstname_sk TEXT, firstname_cs TEXT, firstname_es TEXT, firstname_fi TEXT,
    birthdate DATE, birthcountry TEXT, birthcity_default TEXT, birthstateprovince_default TEXT,
    headshot TEXT, weightinpounds NUMERIC, heightininches NUMERIC, scrapedon TIMESTAMPTZ,
    PRIMARY KEY (id, season)
);

-- 5. SCHEDULE (Literal Match to schedule.csv)
CREATE TABLE schedule (
    id BIGINT PRIMARY KEY, season BIGINT, gametype NUMERIC, gamedate DATE, neutralsite BOOLEAN,
    starttimeutc TEXT, easternutcoffset TEXT, venueutcoffset TEXT, venuetimezone TEXT,
    gamestate TEXT, gameschedulestate TEXT, tvbroadcasts JSONB, threeminrecap TEXT,
    threeminrecapfr TEXT, condensedgame TEXT, condensedgamefr TEXT, gamecenterlink TEXT,
    scrapedon TIMESTAMPTZ, source TEXT, venue_default TEXT, awayteam_id BIGINT,
    awayteam_commonname_default TEXT, awayteam_placename_default TEXT,
    awayteam_placenamewithpreposition_default TEXT, awayteam_placenamewithpreposition_fr TEXT,
    awayteam_abbrev TEXT, awayteam_logo TEXT, awayteam_darklogo TEXT,
    awayteam_awaysplitsquad BOOLEAN, awayteam_score NUMERIC, hometeam_id BIGINT,
    hometeam_commonname_default TEXT, hometeam_placename_default TEXT,
    hometeam_placenamewithpreposition_default TEXT, hometeam_placenamewithpreposition_fr TEXT,
    hometeam_abbrev TEXT, hometeam_logo TEXT, hometeam_darklogo TEXT,
    hometeam_homesplitsquad BOOLEAN, hometeam_score NUMERIC, perioddescriptor_periodtype TEXT,
    perioddescriptor_maxregulationperiods NUMERIC, gameoutcome_lastperiodtype TEXT,
    winninggoalie_playerid NUMERIC, winninggoalie_firstinitial_default TEXT,
    winninggoalie_lastname_default TEXT, awayteam_placename_fr TEXT, winninggoalie_lastname_cs TEXT,
    winninggoalie_lastname_fi TEXT, winninggoalie_lastname_sk TEXT, winninggoalie_lastname_sv TEXT,
    winninggoalscorer_playerid NUMERIC, winninggoalscorer_firstinitial_default TEXT,
    winninggoalscorer_lastname_default TEXT, venue_fr TEXT, hometeam_commonname_fr TEXT,
    awayteam_commonname_fr TEXT, hometeam_hotellink TEXT, hometeam_hoteldesc TEXT,
    awayteam_airlinelink TEXT, awayteam_airlinedesc TEXT, awayteam_hotellink TEXT,
    awayteam_hoteldesc TEXT, hometeam_airlinelink TEXT, hometeam_airlinedesc TEXT,
    hometeam_placename_fr TEXT, ticketslink TEXT, ticketslinkfr TEXT, awayteam_radiolink TEXT,
    hometeam_radiolink TEXT, awayteam_promolink TEXT, awayteam_promodesc TEXT,
    hometeam_promolink TEXT, hometeam_promodesc TEXT, venue_es TEXT
);

-- 6. PLAYS (Literal Match to game.csv)
CREATE TABLE plays (
    id TEXT PRIMARY KEY, gameid BIGINT REFERENCES schedule(id), sortorder NUMERIC,
    per NUMERIC, strength TEXT, event TEXT, description TEXT, time TEXT,
    timeremaining TEXT, timeinperiodsec NUMERIC, timeremainingsec NUMERIC,
    home_on_ice JSONB, away_on_ice JSONB, home_goalie JSONB, away_goalie JSONB,
    eventid NUMERIC, timeinperiod TEXT, timeremaining_api TEXT, situationcode TEXT,
    hometeamdefendingside TEXT, typecode TEXT, event_api TEXT, period NUMERIC,
    periodtype TEXT, maxregulationperiods NUMERIC, teamid_ BIGINT, losingplayerid BIGINT,
    winningplayerid BIGINT, xcoord NUMERIC, ycoord NUMERIC, zonecode TEXT,
    hittingplayerid BIGINT, hitteeplayerid BIGINT, shottype TEXT, shootingplayerid BIGINT,
    goalieinnetid BIGINT, awaysog NUMERIC, homesog NUMERIC, playerid BIGINT,
    reason TEXT, blockingplayerid BIGINT, typecode_1 TEXT, desckey TEXT, duration NUMERIC,
    committedbyplayerid BIGINT, drawnbyplayerid BIGINT, secondaryreason TEXT,
    pptreplayurl TEXT, scoringplayerid BIGINT, scoringplayertotal NUMERIC,
    assist1playerid BIGINT, assist1playertotal NUMERIC, awayscore NUMERIC, homescore NUMERIC,
    highlightclipsharingurl TEXT, highlightclip TEXT, discreteclip TEXT,
    assist2playerid BIGINT, assist2playertotal NUMERIC, ishome BOOLEAN, eventteam TEXT,
    html_event TEXT, home_on_id JSONB, away_on_id JSONB, homegoalie_on_id JSONB,
    awaygoalie_on_id JSONB, home_on_full_name JSONB, away_on_full_name JSONB,
    homegoalie_on_full_name JSONB, awaygoalie_on_full_name JSONB, home_on_count NUMERIC,
    away_on_count NUMERIC, homegoalie_on_count NUMERIC, awaygoalie_on_count NUMERIC,
    n_home_skaters NUMERIC, n_away_skaters NUMERIC, pulled_home NUMERIC, pulled_away NUMERIC,
    home_strength TEXT, away_strength TEXT, gamestrength TEXT, detailedgamestrength TEXT,
    elapsedtime NUMERIC, player1id BIGINT, player2id BIGINT, player3id BIGINT,
    player1name TEXT, player2name TEXT, player3name TEXT, hometeam TEXT, awayteam TEXT,
    teamid BIGINT, isgoalie BOOLEAN, venue TEXT, venuelocation TEXT, gamedate DATE,
    gametype NUMERIC, starttimeutc TEXT, easternutcoffset TEXT, venueutcoffset TEXT,
    scrapedon TIMESTAMPTZ, source TEXT, x_norm NUMERIC, y_norm NUMERIC,
    distancefromgoal NUMERIC, angle_signed NUMERIC, strengthdiff NUMERIC, scorediff NUMERIC,
    shooterskaters NUMERIC, defendingskaters NUMERIC, shootemptynet BOOLEAN,
    previousevent TEXT, previousteam TEXT, previouseventsameteam BOOLEAN,
    previouselapsedtime NUMERIC, previouseventdistancefromgoal NUMERIC,
    previouseventanglesigned NUMERIC, previouseventxnorm NUMERIC, previouseventynorm NUMERIC,
    timediff NUMERIC, isrebound BOOLEAN, isgoal BOOLEAN, xg NUMERIC, "time:elapsed game" TEXT
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

-- 8. STANDINGS (Literal Match to standings.csv)
CREATE TABLE standings (
    id TEXT PRIMARY KEY, date DATE, seasonid BIGINT, conferenceabbrev TEXT,
    conferencehomesequence NUMERIC, conferencel10sequence NUMERIC, conferencename TEXT,
    conferenceroadsequence NUMERIC, conferencesequence NUMERIC, divisionabbrev TEXT,
    divisionhomesequence NUMERIC, divisionl10sequence NUMERIC, divisionname TEXT,
    divisionroadsequence NUMERIC, divisionsequence NUMERIC, gametypeid NUMERIC,
    gamesplayed NUMERIC, goaldifferential NUMERIC, goaldifferentialpctg NUMERIC,
    goalagainst NUMERIC, goalfor NUMERIC, goalsforpctg NUMERIC, homegamesplayed NUMERIC,
    homegoaldifferential NUMERIC, homegoalsagainst NUMERIC, homegoalsfor NUMERIC,
    homelosses NUMERIC, homeotlosses NUMERIC, homepoints NUMERIC,
    homeregulationplusotwins NUMERIC, homeregulationwins NUMERIC, hometies NUMERIC,
    homewins NUMERIC, l10gamesplayed NUMERIC, l10goaldifferential NUMERIC,
    l10goalsagainst NUMERIC, l10goalsfor NUMERIC, l10losses NUMERIC, l10otlosses NUMERIC,
    l10points NUMERIC, l10regulationplusotwins NUMERIC, l10regulationwins NUMERIC,
    l10ties NUMERIC, l10wins NUMERIC, leaguehomesequence NUMERIC, leaguel10sequence NUMERIC,
    leagueroadsequence NUMERIC, leaguesequence NUMERIC, losses NUMERIC, otlosses NUMERIC,
    pointpctg NUMERIC, points NUMERIC, regulationplusotwinpctg NUMERIC,
    regulationplusotwins NUMERIC, regulationwinpctg NUMERIC, regulationwins NUMERIC,
    roadgamesplayed NUMERIC, roadgoaldifferential NUMERIC, roadgoalsagainst NUMERIC,
    roadgoalsfor NUMERIC, roadlosses NUMERIC, roadotlosses NUMERIC, roadpoints NUMERIC,
    roadregulationplusotwins NUMERIC, roadregulationwins NUMERIC, roadties NUMERIC,
    roadwins NUMERIC, shootoutlosses NUMERIC, shootoutwins NUMERIC, streakcode TEXT,
    streakcount NUMERIC, teamlogo TEXT, ties NUMERIC, waiverssequence NUMERIC,
    wildcardsequence NUMERIC, winpctg NUMERIC, wins NUMERIC, scrapedon TIMESTAMPTZ,
    source TEXT, placename_default TEXT, teamname_default TEXT, teamname_fr TEXT,
    teamcommonname_default TEXT, teamabbrev_default TEXT, placename_fr TEXT, teamcommonname_fr TEXT
);

-- 9. DRAFT (Literal Match to draft.csv)
CREATE TABLE draft (
    id TEXT PRIMARY KEY, round NUMERIC, pickinround NUMERIC, overallpick BIGINT,
    teamid BIGINT, teamabbrev TEXT, teamlogolight TEXT, teamlogodark TEXT,
    teampickhistory TEXT, positioncode TEXT, countrycode TEXT, height NUMERIC,
    weight NUMERIC, amateurleague TEXT, amateurclubname TEXT, year BIGINT,
    scrapedon TIMESTAMPTZ, source TEXT, teamname_default TEXT, teamname_fr TEXT,
    teamcommonname_default TEXT, teamplacenamewithpreposition_default TEXT,
    teamplacenamewithpreposition_fr TEXT, displayabbrev_default TEXT,
    firstname_default TEXT, lastname_default TEXT, teamcommonname_fr TEXT
);