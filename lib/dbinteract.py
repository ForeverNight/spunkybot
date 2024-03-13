import sqlite3
import os
import time

# do all the db interactions

class DBInteractor:
    def __init__(self, dbtype, homepath):
        self.dbtype = dbtype

        if self.dbtype == "sqlite3":
            self.interactor = SQLite3Interactor(homepath)

    def openConnection(self):
        self.interactor.openConnection()

    def initializedb(self):
        self.interactor.initializeDB()

    def closeConnection(self):
        self.interactor.closeConnection()

    def getHeadAdminExists(self):
        return self.interactor.getHeadAdminExists()
    
    def removeExpiredEntries(self):
        return self.interactor.removeExpiredEntries()
    
    def getPlayerById(self, playerId):
        return self.interactor.getPlayerById(playerId)
    
    def getXlrTopStats120Days(self):
        return self.interactor.getXlrTopStats120Days()
    
    def getXlrStatsByGuid(self, guid):
        return self.interactor.getXlrStatsByGuid(guid)
    
    def addPlayerToXlrStats(self, guid, name, ipAddress, role, now):
        self.interactor.addPlayerToXlrStats(guid, name, ipAddress, role, now)
    
    def updateXlrStats(self, guid, name, now):
        return self.interactor.updateXlrStats(guid, name, now)
    
    def isDbAlive(self):
        return self.interactor.isDbAlive()
    
    def getActiveBansByPlayerGuid(self, playerGuid):
        return self.interactor.getActiveBansByPlayerGuid(playerGuid)

    def getPlayerByName(self, name):
        return self.interactor.getPlayerByName(name)
    
    def getLast10ActiveBans(self):
        return self.interactor.getLast10ActiveBans()

    def getLast4Bans(self):
        return self.interactor.getLast4Bans()
    
    def getBanByPlayerId(self, playerId):
        return self.interactor.getBanByPlayerId(playerId)
    
    def unbanByPlayerId(self, playerId):
        self.interactor.unbanByPlayerId(playerId)

    def unbanByGuidAndIpAddr(self, guid, ipAddress):
        self.interactor.unbanByGuidAndIpAddr(guid, ipAddress)

    def getBanByGuid(self, guid, now=None):
        return self.interactor.getBanByGuid(guid, now)

    def getBanByIpAddr(self, ipAddress, now=None):
        return self.interactor.getBanByIpAddr(ipAddress, now)
    
    def getCurrentBanExpirationByGuid(self, guid):
        return self.interactor.getCurrentBanExpirationByGuid(guid)
    
    def updateBan(self, guid, ipAddress, expireTime, reason):
        self.interactor.updateBan(guid, ipAddress, expireTime, reason)

    def createBan(self, playerId, guid, name, ipAddress, expireTime, timestamp, reason):
        self.interactor.createBan(playerId, guid, name, ipAddress, expireTime, timestamp, reason)

    def addBanPoint(self, guid, pointType, expireTime):
        self.interactor.addBanPoint(guid, pointType, expireTime)

    def getBanPoints(self, guid, now=None):
        return self.interactor.getBanPoints(guid, now)

    def putInfoInXlrStats(self, guid, kills, deaths, headShots, tkCount, teamDeaths, killingStreak, suicides, ratio):
        self.interactor.putInfoInXlrStats(guid, kills, deaths, headShots, tkCount, teamDeaths, killingStreak, suicides, ratio)

    def isGuidInDb(self, guid):
        return self.interactor.isGuidInDb(guid)
    
    def addPlayer(self, guid, name, address, now):
        return self.interactor.addPlayer(guid, name, address, now)
    
    def updatePlayer(self, guid, name, address, now):
        return self.interactor.updatePlayer(guid, name, address, now)
    
    def getAliasesByGuid(self, guid):
        return self.interactor.getAliasesByGuid(guid)
    
    def updateAliases(self, guid, aliases):
        self.interactor.updateAliases(guid, aliases)

    def getPlayerIdByGuid(self, guid):
        return self.interactor.getPlayerIdByGuid(guid)
    
    def isPlayerRegistered(self, guid):
        return self.interactor.isPlayerRegistered(guid)
    
    def getLastPlayedAndAdminByGuid(self, guid):
        return self.interactor.getLastPlayedAndAdminByGuid(guid)
    
    def setAdminRole(self, guid, role):
        self.interactor.setAdminRole(guid, role)

    def clearBanPointsByGuid(self, guid):
        self.interactor.clearBanPointsByGuid(guid)

class SQLite3Interactor:
    def __init__(self, homepath):
        #do work
        self.homepath = homepath
        self.conn = None
        self.cursor = None

    def openConnection(self):
        self.conn = sqlite3.connect(os.path.join(self.homepath, 'data.sqlite'))
        self.cursor = self.conn.cursor()

    def initializeDB(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS xlrstats (id INTEGER PRIMARY KEY NOT NULL, guid TEXT NOT NULL, name TEXT NOT NULL, ip_address TEXT NOT NULL, first_seen DATETIME, last_played DATETIME, num_played INTEGER DEFAULT 1, kills INTEGER DEFAULT 0, deaths INTEGER DEFAULT 0, headshots INTEGER DEFAULT 0, team_kills INTEGER DEFAULT 0, team_death INTEGER DEFAULT 0, max_kill_streak INTEGER DEFAULT 0, suicides INTEGER DEFAULT 0, ratio REAL DEFAULT 0, rounds INTEGER DEFAULT 0, admin_role INTEGER DEFAULT 1)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS player (id INTEGER PRIMARY KEY NOT NULL, guid TEXT NOT NULL, name TEXT NOT NULL, ip_address TEXT NOT NULL, time_joined DATETIME, aliases TEXT)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ban_list (id INTEGER PRIMARY KEY NOT NULL, guid TEXT NOT NULL, name TEXT, ip_address TEXT, expires DATETIME DEFAULT 259200, timestamp DATETIME, reason TEXT)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS ban_points (id INTEGER PRIMARY KEY NOT NULL, guid TEXT NOT NULL, point_type TEXT, expires DATETIME)')

    def closeConnection(self):
        self.conn.close()

    def getHeadAdminExists(self):
        self.cursor.execute("SELECT COUNT(*) FROM `xlrstats` WHERE `admin_role` = 100")
        return True if int(self.cursor.fetchone()[0]) < 1 else False
    
    def removeExpiredEntries(self):
        self.cursor.execute("DELETE FROM `ban_points` WHERE `expires` < '{}'".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        self.conn.commit()

    def getPlayerById(self, playerId):
        self.cursor.execute("SELECT `guid`,`name`,`ip_address` FROM `player` WHERE `id` = {}".format(int(playerId)))
        return self.cursor.fetchone()
    
    def getXlrTopStats120Days(self):
        self.cursor.execute("SELECT name FROM `xlrstats` WHERE (`rounds` > 35 or `kills` > 500) and `last_played` > '{}' ORDER BY `ratio` DESC LIMIT 3".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime((time.time() - 10368000)))))  # last played within the last 120 days
        return self.cursor.fetchall()
    
    def addPlayerToXlrStats(self, guid, name, ipAddress, role, now=None):
        if now is None:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        self.cursor.execute('INSERT INTO `xlrstats` (`guid`,`name`,`ip_address`,`first_seen`,`last_played`,`num_played`,`admin_role`) VALUES ("{}","{}","{}","{}","{}",1,{})'.format(guid, name, ipAddress, now, now, role))
        self.conn.commit()
    
    def getXlrStatsByGuid(self, guid):
        self.cursor.execute("SELECT `last_played`,`num_played`,`kills`,`deaths`,`headshots`,`team_kills`,`team_death`,`max_kill_streak`,`suicides`,`admin_role`,`first_seen` FROM `xlrstats` WHERE `guid` = '{}'".format(guid))
        return self.cursor.fetchone()
    
    def updateXlrStats(self, guid, name, now):
        self.cursor.execute('UPDATE `xlrstats` SET `name` = "{}",`last_played` = "{}",`num_played` = `num_played` + 1 WHERE `guid` = "{}"'.format(name, now, guid))
        self.conn.commit()
    
    def isDbAlive(self):
        self.cursor.execute("SELECT 1 FROM player LIMIT 1;")
        return bool(self.cursor.fetchall())
    
    def getActiveBansByPlayerGuid(self, playerGuid):
        self.cursor.execute("SELECT `expires` FROM `ban_list` WHERE `expires` > '{}' AND `guid` = '{}'".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), playerGuid))
        return self.cursor.fetchone()
    
    def getPlayerByName(self, name):
        self.cursor.execute("SELECT `id`,`name`,`time_joined` FROM `player` WHERE `name` like '%{}%' ORDER BY `time_joined` DESC LIMIT 8".format(name))
        return self.cursor.fetchall()
    
    def getLast10ActiveBans(self):
        self.cursor.execute("SELECT `id`,`name` FROM `ban_list` WHERE `expires` > '{}' ORDER BY `timestamp` DESC LIMIT 10".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        return self.cursor.fetchall()
    
    def getLast4Bans(self):
        self.cursor.execute("SELECT `id`,`name`,`expires` FROM `ban_list` ORDER BY `timestamp` DESC LIMIT 4")
        return self.cursor.fetchall()

    def getBanByPlayerId(self, playerId):
        self.cursor.execute("SELECT `guid`,`name`,`ip_address` FROM `ban_list` WHERE `id` = {}".format(int(playerId)))
        return self.cursor.fetchone()
    
    def unbanByPlayerId(self, playerId):
        self.cursor.execute("DELETE FROM `ban_list` WHERE `id` = {}".format(int(playerId)))
        self.conn.commit()

    def unbanByGuidAndIpAddr(self, guid, ipAddress):
        self.cursor.execute("DELETE FROM `ban_list` WHERE `guid` = '{}' OR ip_address = '{}'".format(guid, ipAddress))
        self.conn.commit()

    def getBanByGuid(self, guid, now=None):
        if now is None:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        self.cursor.execute("SELECT `id`,`reason` FROM `ban_list` WHERE `guid` = '{}' AND `expires` > '{}'".format(guid, now))
        return self.cursor.fetchone()
    
    def getBanByIpAddr(self, ipAddress, now=None):
        if now is None:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        self.cursor.execute("SELECT `id`,`reason` FROM `ban_list` WHERE `ip_address` = '{}' AND `expires` > '{}'".format(ipAddress, now))
        return self.cursor.fetchone()
    
    def getCurrentBanByGuid(self, guid):
        self.cursor.execute("SELECT `expires`, `reason` FROM `ban_list` WHERE `guid` = '{}'".format(guid))
        return self.cursor.fetchone()
    
    def updateBan(self, guid, ipAddress, expireTime, reason):
        self.cursor.execute("UPDATE `ban_list` SET `ip_address` = '{}',`expires` = '{}',`reason` = '{}' WHERE `guid` = '{}'".format(ipAddress, expireTime, reason, guid))
        self.conn.commit()

    def createBan(self, playerId, guid, name, ipAddress, expireTime, timestamp, reason):
        self.cursor.execute('INSERT INTO `ban_list` (`id`,`guid`,`name`,`ip_address`,`expires`,`timestamp`,`reason`) VALUES ({},"{}","{}","{}","{}","{}","{}")'.format(playerId, guid, name, ipAddress, expireTime, timestamp, reason))
        self.conn.commit()

    def addBanPoint(self, guid, pointType, expireTime):
        self.cursor.execute("INSERT INTO `ban_points` (`guid`,`point_type`,`expires`) VALUES ('{}','{}','{}')".format(guid, pointType, expireTime))
        self.conn.commit()

    def getBanPoints(self, guid, now=None):
        if now is None:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        self.cursor.execute("SELECT COUNT(*) FROM `ban_points` WHERE `guid` = '{}' AND `expires` > '{}'".format(guid, now))
        return self.cursor.fetchone()
    
    def putInfoInXlrStats(self, guid, kills, deaths, headShots, tkCount, teamDeaths, killingStreak, suicides, ratio):
        self.cursor.execute("UPDATE `xlrstats` SET `kills` = {},`deaths` = {},`headshots` = {},`team_kills` = {},`team_death` = {},`max_kill_streak` = {},`suicides` = {},`rounds` = `rounds` + 1,`ratio` = {} WHERE `guid` = '{}'".format(kills, deaths, headShots, tkCount, teamDeaths, killingStreak, suicides, ratio, guid))
        self.conn.commit()

    def isGuidInDb(self, guid):
        self.cursor.execute("SELECT COUNT(*) FROM `player` WHERE `guid` = '{}'".format(guid))
        return int(self.cursor.fetchone()[0]) != 0
    
    def addPlayer(self, guid, name, address, now):
        # add new player to database
        self.cursor.execute('INSERT INTO `player` (`guid`,`name`,`ip_address`,`time_joined`,`aliases`) VALUES ("{}","{}","{}","{}","{}")'.format(guid, name, address, now, name))
        self.conn.commit()

    def updatePlayer(self, guid, name, address, now):
        self.cursor.execute('UPDATE `player` SET `name` = "{}",`ip_address` = "{}",`time_joined` = "{}" WHERE `guid` = "{}"'.format(name, address, now, guid))
        self.conn.commit()

    def getAliasesByGuid(self, guid):
        self.cursor.execute('SELECT `aliases` FROM `player` WHERE `guid` = "{}"'.format(guid))
        return self.cursor.fetchone()
    
    def updateAliases(self, guid, aliases):
        self.cursor.execute('UPDATE `player` SET `aliases` = "{}" WHERE `guid` = "{}"'.format(aliases, guid))
        self.conn.commit()

    def getPlayerIdByGuid(self, guid):
        self.cursor.execute("SELECT `id` FROM `player` WHERE `guid` = '{}'".format(guid))
        return self.cursor.fetchone()[0]
    
    def isPlayerRegistered(self, guid):
        self.cursor.execute("SELECT COUNT(*) FROM `xlrstats` WHERE `guid` = '{}'".format(guid))
        return self.cursor.fetchone()[0] == 0
    
    def getLastPlayedAndAdminByGuid(self, guid):
        self.cursor.execute("SELECT `last_played`,`admin_role` FROM `xlrstats` WHERE `guid` = '{}'".format(guid))
        return self.cursor.fetchone()
    
    def setAdminRole(self, guid, role):
        self.cursor.execute("UPDATE `xlrstats` SET `admin_role` = {} WHERE `guid` = '{}'".format(role, guid))
        self.conn.commit()

    def clearBanPointsByGuid(self, guid):
        self.cursor.execute("DELETE FROM `ban_points` WHERE `guid` = '{}' and `expires` > '{}'".format(guid, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        self.conn.commit()