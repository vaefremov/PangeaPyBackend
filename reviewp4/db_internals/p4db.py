# $Id: p4db.py 16671 2015-12-09 18:39:35Z efremov $
"""
Base module to access PANGEA ReView database
"""
__version__ = "$Revision: 16671 $"[11:-2]  # code version

import os
import os.path
import types
import collections
from .p4dbexceptions import DBException, DBCorruptionException, DBAuthoritiesException, DBNotFoundException
from .classes_def import className2classIdString

############################################################
## Required version of storage DataModel
VERSION_MAJOR = 1
VERSION_MINOR = 62
MAXFLOAT = 3.40282347e+38 ## stands for undefined values of parameters

def makeUniqueNameNew(name, nm_list, separator, max_try = 100):
    """Makes new name not in the list nm_list.
    Unique name if made by adding integer index prefixed with separator
    to the end of the name. In the case the name is already composite
    name with separator and index at the end, index is incremented. E.g.
    f0_1 may result in f0_2 (provided separator is _)
    """
    nm_list_upper = [n.upper() for n in nm_list]
    if not (name.upper() in nm_list_upper):
        return name
    # try to split name
    ndx = name.rfind(separator)
    if ndx == -1:
        start_ndx = 1
    else:
        try_start_ndx_s = name[ndx+1:]
        try_start_ndx = None
        try:
            try_start_ndx = int(try_start_ndx_s)
        except ValueError:
            start_ndx = 1
        else:
            name = name[:ndx]
            start_ndx = try_start_ndx + 1

    for i in range(start_ndx, max_try):
        try_nm = name + ("%s%i" % ( separator, i))
        if not (try_nm.upper() in nm_list_upper):
            return try_nm
    raise RuntimeError('Maximum number of tries %d reached' % max_try)


class Authorities:
    """Encapsulates authorities checks and operations"""
    # Operations
    CREATE_PROJ = 'CREATE_PROJ'
    DELETE_PROJ = 'DELETE_PROJ'
    ACCESS_PROJ = 'ACCESS_PROJ'
    CREATE_AREA = 'ProjAdm'
    DELETE_AREA = 'ProjAdm'
    CREATE_CONT = 'CrtCont'
    UPDT_CONT   = 'UpdCont'
    DELETE_CONT = 'DelCont'
    CREATE_ATTR = 'CrtAttr'
    UPDT_ATTR   = 'UpdAttr'
    DELETE_ATTR = 'DelAttr'
    ADMIN_PROJ  = 'ProjAdm'

    def __init__(self, connection, a_user):
        """Input:
            a_cursor - current cursor in database
            a_user - user name
            a_passwd - password of user"""
        self.c = connection.cursor()
        self.userid = None
        self.changeUser(a_user)

    def changeUser(self, a_user):
        """Change user name and password"""
        self.user = a_user
        self.userid = self.getID()

    def getPermissions(self, containerID):
        """Returns permissions set for user relative to project that containerID belongs to"""
        # 1-st = check if operation does not affect any project
        if containerID == 0:
            # impossible proj. ID - default permissions
            return [Authorities.CREATE_PROJ]
        n = self.c.execute("""SELECT p.PermissionFlags from UserPermissions p, Containers c 
                           where p.UserID = %d and p.ContainerLink = c.TopParent 
                           and c.CodeContainer = %d """ % (self.userid, containerID))
        if n == 0:
            # no permissions - deny operation
            return []
        flags_str = self.c.fetchall()[-1][0]
        if flags_str is None:
            # default permissions for project - read only
            return [Authorities.ACCESS_PROJ]
        flags = flags_str.split(',')
        # adding default rights
        flags.append(Authorities.ACCESS_PROJ)
        if Authorities.ADMIN_PROJ in flags:
            flags.append(Authorities.DELETE_PROJ)
        return flags
    
        
    def getRole(self, projID):
        """Return string representing a role of user against the given project.
        Output may be: "Administrator", "User", "Viewer", "NoAccess", "Super"
        """
        flags = self.getPermissions(projID)
        if not flags:
            return 'NoAccess'
        if 'ProjSuper' in flags:
            return 'Super'
        if 'ProjAdm' in flags:
            return 'Administrator'
        if ('ProjSuper' in flags) or [x for x in flags if (x.find('Del') != -1) or (x.find('Upd') != -1)]:
            return 'User'
        return 'Viewer'

    def assertNotProtected(self, containerID):
        """Check if container is not protected, raise exception otherwise.
        """
        if self.isProtected(containerID):
            raise DBAuthoritiesException('Protected container %d cannot be deleted or changed!' % containerID)

    def isProtected(self, containerID):
        """Check if container is protected.
        """
        n = self.c.execute("SELECT isProtected FROM Containers WHERE CodeContainer = %d" % containerID)
        if n:
            if self.c.fetchall()[0][0]:
                return True
        return False

    def assertUserOwnsContainer(self, containerID):
        """Assert that container is owned by current user.
        """
        if not self.isUserOwnsContainer(containerID):
            raise DBAuthoritiesException('Container %d is not owned by current user %s!' % (containerID, self.user))

    def assertUserOwnsContainerOrIsAdmin(self, containerID):
        """Should be used to check if user has permissions to delete of edit container """
        if not self.isUserOwnsContainerOrIsAdmin(containerID):
            raise DBAuthoritiesException('Container %d is not owned by current user %s or user is not Admin!' % (containerID, self.user))

    def isUserOwnsContainerOrIsAdmin(self, containerID):
        """Returns true if user owns container or is admin or super """
        return self.isUserOwnsContainer(containerID) or self.isAdministratorRole(containerID)

    def isUserOwnsContainer(self, containerID):
        """Verify if a container is owned by current user.
        """
        n = self.c.execute("SELECT ownerID FROM Containers WHERE CodeContainer = %d" % containerID)
        if n:
            owner = self.c.fetchall()[0][0]
            return owner == self.userid
        return False

    def isSuperRole(self, projID):
        """Checks, if user can do administrative actions. Return True or False.
        """
        flags = self.getPermissions(projID)
        return 'ProjSuper' in flags

    def checkSuperRole(self, projID):
        """Checks, if user can do administrative actions. Raise exception if he cannot.
        """
        if not self.isSuperRole(projID):
            raise DBAuthoritiesException('Denied operation for user ID %d in project ID %d, only superuser can do that' % (self.userid, projID))        

    def isAdministratorRole(self, projID):
        """Checks, if user can do administrative actions. Return True or False.
        """
        flags = self.getPermissions(projID)
        return ('ProjAdm' in flags) or self.isSuperRole(projID)

    def checkAdministratorRole(self, projID):
        """Checks, if user can do administrative actions. Raise exception if he cannot.
        """
        if not self.isAdministratorRole(projID):
            raise DBAuthoritiesException('Denied operation for user ID %d in project ID %d, only administrator can do that' % (self.userid, projID))        


    def checkPermissions(self, projID, operation, flags = None):
        """Checks permission to perform operation
        Input:
            projID - ID of project
            operation - operation code
            flags - permissions obtained earlier with getPermissions call
        Output:
            1 if permission granted, DBAuthoritiesException otherwise"""
        if flags is None:
            flags = self.getPermissions(projID)
        # now - really check permissions
        if operation in flags:
            return 1
        else:
            raise DBAuthoritiesException('Denied operation %s for user ID %d in project ID %d' % (operation, self.userid, projID))

        if operation == Authorities.DELETE_PROJ:
            if 'ProjSuper' in flags:
                return 1
            else:
                raise DBAuthoritiesException('Denied operation %s for user ID %d in project ID %d, only administrator can do that' % (operation, self.userid, projID))
        elif operation == Authorities.ACCESS_PROJ:
            return 1
        elif operation in flags:
            return 1
        else:
            raise DBAuthoritiesException('Denied operation %s for user ID %d in project ID %d' % (operation, self.userid, projID))
        return 1

    def getID(self):
        """Returns ID of user"""
        if self.userid:
            return self.userid
        n = self.c.execute("""SELECT UserID FROM Users
                WHERE UserName = '%s' """ % self.user)
        if not n:
            raise DBAuthoritiesException('Authority exception for user %s' % self.user)
        return self.c.fetchall()[0][0]

    def getComment(self):
        """Gets additional info about user"""
        n = self.c.execute("""SELECT UserComment FROM Users WHERE UserID = %d """ % self.userid)
        if not n:
            raise DBAuthoritiesException('DBAuthoritiesException: wrong user ID %d' % self.userid)
        return self.c.fetchall()[0][0]

    def addLog(self, objType, objID, operation = 'Create'):
        """Add string to changelog table describing operation"""
        # !!!efremov - need to check if object type is valid
        n = self.c.execute("""INSERT ChangeLog(TableType, UserID, Link, Operation)
                VALUES("%s", %d, %d, "%s") """ % (objType, self.userid, objID, operation))
    
    def setProtectionFlag(self, objID, flag):
        "Sets protection flag for container designated by objID"
        assert (flag == 0) or (flag == 1), 'Illegal value of protection flag!'
        self.c.execute("UPDATE Containers SET isProtected = %d WHERE CodeContainer = %d" % (flag, objID))


    def setOwner(self, objID, ownerName):
        "Set owner of object (container) designated by objID"
        n = self.c.execute("SELECT TopParent FROM Containers WHERE CodeContainer = %d" % objID)
        assert n == 1, "Container with the ID %d does not exist or there are duplicates!"
        projectID = self.c.fetchall()[0][0]
        self.checkSuperRole(projectID)
        if ownerName:
            n = self.c.execute("SELECT UserID FROM USERS WHERE UserName = '%s'" % ownerName)
            assert n == 1, "User %s does not exist" % ownerName
            newOwnerID = self.c.fetchall()[0][0]
        else:
            newOwnerID = self.getID()
        self.c.execute("UPDATE Containers SET ownerID = %d WHERE CodeContainer = %d" % (newOwnerID, objID))

    def makeOwnerCurrentIfAdmin(self, objID):
        """Set owner of object to current user. This operation is performed only if the current user is admin relative to the project objID belongs to. """       
        if self.isUserOwnsContainerOrIsAdmin(objID):
            self.c.execute("UPDATE Containers SET ownerID = %d WHERE CodeContainer = %d" % (self.userid, objID))


class AuthoritiesAdmin:
    "Interface for administration of users"
    def __init__(self, connection, a_user):
        """Create Authorities instance"""
        self.connection = connection
        self.c = self.connection.cursor()
        self.user = a_user
        self.auth = Authorities(self.connection, self.user)

    def setPermissions(self, projID, permissions):
        """Set permissions for project and current user"""
        userid = self.auth.getID()
        n = self.c.execute("""SELECT UserID, ContainerLink, PermissionFlags FROM UserPermissions
                WHERE UserID = %d
                AND ContainerLink = %d """ % (userid, projID))
        perm_str = ','.join(permissions)
        if perm_str == '':
            perm_str = None
        if n:
            (userid, pr_id, flags) = self.c.fetchall()[0]
            if perm_str is None:
                self.c.execute("""UPDATE UserPermissions
                        SET PermissionFlags = NULL
                        WHERE UserID = %d
                        AND ContainerLink = %d """ % (userid, projID))
            else:
                self.c.execute("""UPDATE UserPermissions
                        SET PermissionFlags = "%s"
                        WHERE UserID = %d
                        AND ContainerLink = %d """ % (perm_str, userid, projID))
        else:
            if perm_str is None:
                self.c.execute("""INSERT UserPermissions (UserID, ContainerLink, PermissionFlags)
                        VALUES(%d, %d, NULL)""" % (userid, projID))
            else:
                self.c.execute("""INSERT UserPermissions (UserID, ContainerLink, PermissionFlags)
                        VALUES(%d, %d, "%s")""" % (userid, projID, perm_str))

    def deletePermissions(self, projID):
        """Delete all permissions relating to projID and
        current user"""
        userid = self.auth.getID()
        n = self.c.execute("""DELETE FROM UserPermissions
                WHERE UserID = %d
                AND ContainerLink = %d """ % (userid, projID))

    # def close(self):
    #     """Closes connection"""
    #     self.c.close()
    #     self.connection.close()
    #     self.connection = None
    #     self.c = None
    #     self.auth = None

# ============== End of class Authorities ====================

knownAttributes = ['bgcolor', 'bgon', 'bordercolor', 'borderon', 'borderthick', 'bottom', 'boundaries', 'cdp',
                   'cdpStep', 'color', 'comment', 'computation', 'coords', 'cube', 'cubeRef', 'dPath',
                   'dependent', 'description', 'dx', 'dy', 'end', 'fgcolor', 'field', 'fill', 'format',
                   'geometry', 'host', 'inDataSet', 'isClosed', 'lastModified', 'line', 'lineRef', 'lithology',
                   'max', 'mdTimeAbsD', 'min', 'mode', 'nx', 'ny', 'origin', 'outDataSet', 'pad', 'parent',
                   'path', 'pathError', 'pathOut', 'pathRunArgs', 'pattern', 'picturePath', 'points',
                   'production', 'rc', 'ref2are1', 'ref2doc1', 'ref2flt', 'ref2hor', 'ref2par', 'ref2parent',
                   'refCDP', 'refSP', 'refs2fltc', 'refs2fltl', 'refs2horc', 'refs2horl', 'refs2layc',
                   'refs2layl', 'refs2wells', 'shortComment', 'spDir', 'start', 'startInline', 'startXline',
                   'stretchPoints', 'stroke', 'style', 'thickness', 'top', 'trajectoryD', 'trajectoryT',
                   'transparency', 'type', 'units', 'uom', 'user', 'validPoints', 'value', 'well', 'x',
                   'xShift', 'y', 'yShift', 'zCompression', 'zShift', 'zType', 'brushColor', 'lineStyle',
                   'ticksStyle', 'zLevel', 'alias', 'refWavelet', "waveletType", "step", "waveletLength",
                   "frequency", "attenuation", "phase", "inversePolarity", "filtration", "filterParameters",
                   'autocorTimeStart', 'autocorTimeEnd', 'autocorDataName', 'autocorQuenching',
                   'corrIntervalUp', 'corrIntervalDown', 'corrCoeff']

legacyAttributes = ['Path', 'Type', 'Line', 'CDP', 'XShift', 'YShift', 'ZShift', 'ZCompression', 'LineRef', 'X', 'Y',
                    'Cube', 'CubeRef', 'Geometry', 'Refs2horl', 'Refs2horc', 'Comment', 'Refs2fltl', 'Refs2fltc',
                    'color', 'thickness', 'mode', 'Coords', 'Origin', 'Dx', 'Dy', 'Nx', 'Ny', 'TrajectoryT',
                    'TrajectoryD', 'ZType', 'Ref2par', 'Value', 'StretchPoints', 'User', 'Start', 'End',
                    'Ref2doc1', 'PathRunArgs', 'PathError', 'PathOut', 'ShortComment', 'RC', 'Host',
                    'PicturePath', 'OutDataSet', 'InDataSet', 'StartInline', 'StartXline', 'CDPStep',
                    'RefCDP', 'RefSP', 'Refs2layl', 'Refs2layc', 'bgcolor', 'bgon', 'transparency',
                    'fgcolor', 'pattern', 'lithology', 'borderon', 'bordercolor', 'borderthick', 'thickness',
                    'thickness', 'fill', 'style', 'style', 'SPDir', 'Ref2are1', 'Ref2hor', 'LastModified',
                    'Description', 'min', 'max', 'units', 'format', 'DPath', 'top', 'bottom', 'ref2parent',
                    'ref2parent', 'computation', 'parent', 'dependent', 'Ref2flt', 'Points', 'validPoints',
                    'production', 'well', 'well', 'field', 'Refs2wells', 'boundaries', 'uom', 'isClosed',
                    'stroke', 'MdTimeAbsD', 'pad', 'brushColor', 'lineStyle', 'ticksStyle', 'zLevel', 'alias']

knownAttrD = dict([(s.lower(), s) for s in knownAttributes])
legacyAttrD = dict([(s.lower(), s) for s in legacyAttributes])

## Exceptions and complements:
legacyAttrD['refToDocument'.lower()] = 'Ref2doc1'
legacyAttrD['userId'.lower()] = 'user'
legacyAttrD['startTime'.lower()] = 'start'
legacyAttrD['endTime'.lower()] = 'end'

new2legacyD = dict([(a, legacyAttrD.get(a.lower()))  for a in list(knownAttrD.values())])


def new2LegacyAttributeName(new_name):
    return new2legacyD.get(new_name) or new_name

def convertAttributeNames2Legacy(d):
    ans = {}
    for newAttrName in d:
        ans[new2LegacyAttributeName(newAttrName)] = d[newAttrName]
    return ans
    

class MetaDataDict(collections.UserDict):
    'Class like dictionary, but ignoring case of keys. Onky string mau be used as keys!'
    def __init__(self, init_data = None, casingDict = {}):
        self.data = {}
        self.casingDict = casingDict
        if init_data is None:
            return
        for key in init_data:
            self.data[self.casingDict.get(key.lower()) or key.lower()] = init_data[key]

    def __setitem__(self, key, item):
        self.data[self.casingDict.get(key.lower()) or key.lower()] = item

    def __getitem__(self, key):
        return super().__getitem__(self.casingDict.get(key.lower()) or key.lower())


class P4DBbase:
    "Class for base operations with P4 DB"
    def __init__(self, connection, user=None):
        """Create wrapper for connection to query the ReView DB.
        Input:
            connection: open connection to DB
            (opt) user: user name in DB
        """
        self.connection = connection
        self.c = self.connection.cursor()
        self.auth = None
        if user:
            self.set_user(user)        
        
        # Check for right version of data model
        self.c.execute("SELECT * FROM ParamTable")
        ver = self.c.fetchall()[0]
        if ver[0] < VERSION_MAJOR:
            raise DBException('Wrong version of datamodel: need %d.%d or better, got %d.%d'
                              % (VERSION_MAJOR, VERSION_MINOR, ver[0], ver[1]) )
        if ver[1] < VERSION_MINOR:
            raise DBException('Wrong version of datamodel: need %d.%d or better, got %d.%d'
                              % (VERSION_MAJOR, VERSION_MINOR, ver[0], ver[1]) )
            
        self.c.execute("SELECT CodeContainerType, NameContainerType FROM ContainerTypes")
        ConTypes = self.c.fetchall()
        self.ContainerTypes = {}
        self.MetaData = {}

        for ConType in ConTypes:
            self.ContainerTypes[ConType[0]] = ConType[1]

            self.c.execute("""SELECT KeyWord, TypeData, DotPosition, Dimension, CodeData, ReferencedContainerType, LinkPermission
                           FROM MetaData WHERE ContainerType = "%s" """ % ConType[0])
            m_d = self.c.fetchall()
##            self.MetaData[ConType[0]] = {}
            # We use special type of dictionary which ignores case of keys when seting or
            # getting values, but preserves casing when asked about set of keys or iterated over.
            # Case considered right is taken from knownAttributes list
            self.MetaData[ConType[0]] = MetaDataDict(None, dict([(s.lower(), s) for s in knownAttributes]))

            for item in m_d:
                self.MetaData[ConType[0]][item[0]] = (item[1],item[2],item[3],item[4],item[5],item[6])

    def set_user(self, user):
        self.auth = Authorities(self.connection, user)

    def getConnection(self):
        """Gets connection to database. Protected function -
        should be reloaded to change real DB user"""
        return self.connection
    
    def commit(self):
        """Commit transaction and restore cursor
        """
        self.connection.commit()

    # def close(self):
    #     "Closing connection"
    #     self.c.close()
    #     self.connection.close()
    #     self.connection = None
    #     self.c = None

    # def __del__(self):
    #     self.close()


    def getDataModelVersion(self):
        """Returns major and minor version of the datamodel, and version date.
        Output data are packed into tuple.
        """
        self.c.execute("SELECT * FROM ParamTable")
        ver = self.c.fetchall()[0]
        return (ver[0], ver[1], str(ver[2])[:10])

    def getContainerTypeAttributes(self, containerType):
        """Returns information about types of container's attributes.
        """
        m_d = self.MetaData[self.type2classIdString(containerType)]
        res = []
        for a in m_d:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a]
            aType = 'Unknown'
            if form == 'D' or form == 'F':
                aType = 'Double'
            elif form == 'I':
                aType = 'Int'
            elif form == 'C':
                aType = 'String'
            elif form == 'P':
                aType = 'Point'
            elif form == 'T':
                aType = 'DateTime'
            elif form == 'R':
                aType = 'Reference<%s>' % self.classIdString2type( ref_type )
            elif form == 'X':
                aType = 'Pair<Double, Reference<%s>>' % self.classIdString2type( ref_type )
            if dim:
                aType = 'Array<' + aType + '>'
            res.append([a, aType])
        return res

    def getUpperContainerType(self, containerType):
        '''Returns type of upper container (well, first of them, because there should be only one parent container). '''
        classId = self.type2classIdString(containerType)
        sql = """SELECT ContainerTypeMaster FROM ContainerTypeSubmission WHERE ContainerTypeSlave='%s'  """ % classId
        self.c.execute(sql)
        res = self.classIdString2type(self.c.fetchall()[0][0])
        return res

    def getLowerContainerTypes(self, containerType):
        'Returns types of containers that can be placed inside container of the given type.'
        classId = self.type2classIdString(containerType)
        sql = """SELECT ContainerTypeSlave FROM ContainerTypeSubmission WHERE ContainerTypeMaster ='%s'  """ % classId
        self.c.execute(sql)
        res = [self.classIdString2type(t[0]) for t in self.c.fetchall()]
        return res
        

    def projectExists(self, project_name):
        """Check if project with given name exists in database.
        Returns 1 if project does exist, 0 otherwise.
        Exceptions: DBCorruptionException if there are duplicate names.
        """
        n = self.c.execute("""SELECT CodeContainer FROM Containers
            WHERE ContainerType = "proj"
            AND ContainerName = "%s" AND Status = "Actual" """ % project_name)
        if n == 0:
            return 0
        elif n > 1:
            raise DBCorruptionException('Duplicate project name %s in database' % project_name)
        return 1
        

    def createProject(self, p_name):
        """Creates project with name a_name. Project name must be unique
        among projects.
        Return:
            ID of new project"""
        # check permissions
        self.auth.checkPermissions(0, Authorities.CREATE_PROJ)
        # Strip and check the validity of name
        p_name = p_name.strip()
        if not p_name:
            raise DBException('Creating project with empty name attempted)')
        self.c.execute("""LOCK TABLES Containers WRITE, ChangeLog WRITE, Users WRITE, UserPermissions WRITE""") # ChangLog is locked to keep MySQL quiet
        try:
            try:
                if self.projectExists(p_name):
                    raise DBException('Duplicate project name %s' % p_name)
            except DBException as e:
                raise e
            sql = """INSERT Containers (LinkUp, TopParent, ContainerType, ContainerName, ownerID)
            VALUES (1, 0, 'proj', '%s', %d)""" % (p_name, self.auth.getID())
            self.c.execute(sql)
            self.c.execute('SELECT LAST_INSERT_ID()')
            new_id = self.c.fetchall()[0][0]
            self.c.execute("""UPDATE Containers
                    SET TopParent = %d WHERE CodeContainer = %d """ % (new_id, new_id))
            self.auth.addLog('Containers', new_id)
            # set permissions for new project
            auth_adm = AuthoritiesAdmin(self.connection, self.auth.user)
            auth_adm.setPermissions(new_id, [Authorities.ADMIN_PROJ,
                                             Authorities.CREATE_CONT,
                                             Authorities.UPDT_CONT, Authorities.DELETE_CONT,
                                             Authorities.CREATE_ATTR,
                                             Authorities.UPDT_ATTR, Authorities.DELETE_ATTR])
            auth_adm_backup = AuthoritiesAdmin(self.connection, 'BACKUP')
            auth_adm_backup.setPermissions(new_id, [])
            return new_id
        finally:   # To be compatible with Python 2.4 we have to use the additional try: finally block
            self.c.execute("""UNLOCK TABLES""")

    def deleteProject(self, p_name):
        """Deletes project (and all containers within)
        """
        p_id = self.getProjectByName(p_name)
        self.auth.checkPermissions(p_id, Authorities.DELETE_PROJ)
        self.markContainersTreeDeleted_NEW(p_id)


    def getContainerProtectionOwnerByName(self, a_parentID, a_type, a_name):
        """Finds ID of container with parent ID denoted by
        a_parentID parameter, type a_type and name a_name
        Output:
          tuple: (ID of container found, protectionFlag, name of owner, may be None)
        Exceptions:
          DBException if cannot find container or there is more than 1
          active containers with the same name.
        """
        if a_type:
            if type(a_type) == list:
                c_types = ','.join(['"' + s + '"' for s in a_type])
                sql = """SELECT TopParent, CodeContainer, isProtected, u.UserName
                     FROM Containers LEFT JOIN Users AS u ON ownerID = u.UserID
                     WHERE LinkUp = %d
                     AND ContainerName = "%s"
                     AND ContainerType IN (%s)
                     AND Status = "Actual" """ % (a_parentID, a_name, c_types)
            else:
                sql = """SELECT TopParent, CodeContainer, isProtected, u.UserName
                     FROM Containers LEFT JOIN Users AS u ON ownerID = u.UserID
                     WHERE LinkUp = %d
                     AND ContainerName = "%s"
                     AND ContainerType = "%s"
                     AND Status = "Actual" """ % (a_parentID, a_name, a_type)
        else:
            sql = """SELECT TopParent, CodeContainer, isProtected, u.UserName
                     FROM Containers LEFT JOIN Users AS u ON ownerID = u.UserID
                     WHERE LinkUp = %d
                     AND ContainerName = "%s"
                     AND Status = "Actual" """ % (a_parentID, a_name)
        n = self.c.execute(sql)
        if n == 0:
            raise DBNotFoundException('ERROR: Cannot find container type %s, parent  %d, name %s' % (a_type, a_parentID, a_name))
        if n > 1:
            raise DBException('ERROR: Duplicate container name %s for parent id  %d' % (a_name, a_parentID))
        tmp = self.c.fetchall()[0]
        pr_id = tmp[0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return tmp[1:]

    def getContainerByName(self, a_parentID, a_type, a_name):
        """Finds ID of container with parent ID denoted by
        a_parentID parameter, type a_type and name a_name
        Note: a_type parameter currently ignored!
        Output:
          ID of container found
        Exceptions:
          DBException if cannot find container or there is more than 1
          active containers with the same name.
        """
        if a_type:
            if type(a_type) == list:
                c_types = ','.join(['"' + s + '"' for s in a_type])
                sql = """SELECT CodeContainer, TopParent
                     FROM Containers
                     WHERE LinkUp = %d
                     AND ContainerName = "%s"
                     AND ContainerType IN (%s)
                     AND Status = "Actual" """ % (a_parentID, a_name, c_types)
            else:
                sql = """SELECT CodeContainer, TopParent
                     FROM Containers
                     WHERE LinkUp = %d
                     AND ContainerName = "%s"
                     AND ContainerType = "%s"
                     AND Status = "Actual" """ % (a_parentID, a_name, a_type)
        else:
            sql = """SELECT CodeContainer, TopParent
                     FROM Containers
                     WHERE LinkUp = %d
                     AND ContainerName = "%s"
                     AND Status = "Actual" """ % (a_parentID, a_name)
        n = self.c.execute(sql)
        if n == 0:
            raise DBNotFoundException('ERROR: Cannot find container type %s, parent  %d, name %s' % (a_type, a_parentID, a_name))
        if n > 1:
            raise DBException('ERROR: Duplicate container name %s for parent id  %d' % (a_name, a_parentID))
        (obj_uid, pr_id) = self.c.fetchall()[0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return obj_uid

    def getProjectByName(self, project_name):
        """Returns ID of project with name project_name"""
        n = self.c.execute("""SELECT CodeContainer FROM Containers
            WHERE ContainerType = "proj"
            AND ContainerName = "%s" AND Status = "Actual" """ % project_name)
        if n == 0:
            raise DBNotFoundException('Cannot find project %s' % project_name)
        elif n > 1:
            raise DBCorruptionException('Duplicate project name %s in database' % project_name)
        proj_uid = self.c.fetchall()[0][0]
        self.auth.checkPermissions(proj_uid, Authorities.ACCESS_PROJ)
        return proj_uid

    def getAreaByName(self, project_name, area_name):
        """Returns the ID for named area belonging to given project"""
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        ar_id = self.getContainerByName(pr_id, 'area', area_name)
        return ar_id

    def getProjectsList(self):
        """Returns list of projects IDs in database
        Only projects available to current user are listed.
        Output:
           [(pPI, name), ...]
        """
        n = self.c.execute("""SELECT CodeContainer, ContainerName FROM Containers
                WHERE ContainerType = "proj" AND Status = "Actual" """)
        if not n:
            return []
        ids_raw = self.c.fetchall()
        ids = []
        for i in ids_raw:
            try:
                self.auth.checkPermissions(i[0], Authorities.ACCESS_PROJ)
                ids.append(i)
            except DBAuthoritiesException:
                pass
        return ids

    def getParentContainer(self, containerID):
        """Returns ID of upper container
        Input:
            containerID - ID of current container"""
        sql = """SELECT LinkUp, TopParent FROM Containers
                WHERE CodeContainer = %d """ % containerID
        n = self.c.execute(sql)
        if n == 0:
            raise DBNotFoundException('No parent containers for %d' % containerID)
        if n > 1:
            raise DBCorruptionException('Multiple or none parent containers for %d' % containerID)
        (parent, pr_id) = self.c.fetchall()[0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return parent

    def getParentProject(self, containerID):
        """Returns ID of project container belongs to"""
        n = self.c.execute("""SELECT TopParent FROM Containers
                WHERE CodeContainer = %d """ % containerID)
        if n == 0:
            raise DBNotFoundException('Multiple or none top parent containers for %d' % containerID)
        if n > 1:
            raise DBCorruptionException('Multiple or none top parent containers for %d' % containerID)
        pr_id = self.c.fetchall()[0][0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return pr_id

    def getContainerStatus(self, containerID):
        """Returns status of container. Status may be 'Actual', 'Deleted'
        or 'Archived'"""
        n = self.c.execute("""SELECT Status, TopParent FROM Containers
                    WHERE CodeContainer = %d """ % containerID)
        if not n:
            raise DBNotFoundException('Cannot find container with ID %d' % containerID)
        (status, pr_id) = self.c.fetchall()[0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return status

    def createContainer(self, container_up, a_type, a_name):
        """Crate new contaner as subordinate of
        container_up.
        Input:
            conainer_up - ID of upper container
            a_type - type of container
            a_name - name of container
        Return:
            The ID of new container
        Exceptions:
            DBException - if wrong type or new container of
            given type cannot belong to upper container.
        """
        # define parent container type
        n = self.c.execute("""SELECT ContainerType, TopParent
            FROM Containers
            WHERE CodeContainer = %d""" % container_up)
        if not n:
            raise DBNotFoundException('Cannot find parent container ID %d' % container_up)
        ret_string = self.c.fetchall()[0]

        up_type = ret_string[0]
        proj_id = ret_string[1]
        self.auth.checkPermissions(proj_id, Authorities.CREATE_CONT)
        # Strip and check the validity of name
        a_name = a_name.strip()
        if not a_name:
            raise DBException('Creating container with empty name attempted (parent container ID %d)' % container_up)
        # Check subordinance
        n = self.c.execute("""SELECT *
            FROM ContainerTypeSubmission
            WHERE ContainerTypeMaster = "%s"
            AND ContainerTypeSlave = "%s" """ % (up_type, a_type))
        if n == 0:
            raise DBException('Wrong type submission (or invalid type): %s %s' % (up_type, a_type))
        # actualy create container
        # 1-st - lock tables so that nobody interferes
        self.c.execute('LOCK TABLES Containers WRITE, ChangeLog WRITE')
        try:
            # check for duplicate names
            sql = """SELECT CodeContainer FROM Containers
                    WHERE LinkUp = %d
                    AND ContainerName = "%s"
                    AND Status = "Actual" """ % (container_up, a_name, )
            n = self.c.execute(sql)
            if n:
                # duplicate names among actual containers found
                raise DBException('Duplicate name %s in container with id %d' % (a_name, container_up))
            sql = """INSERT Containers (LinkUp, TopParent, ContainerType, ContainerName, ownerID)
                VALUES (%d, %d, '%s', '%s', %d) """ % (container_up, proj_id, a_type, a_name, self.auth.getID())
            self.c.execute(sql)
            self.c.execute('SELECT LAST_INSERT_ID()')
            new_id = self.c.fetchall()[0][0]
            self.auth.addLog('Containers', new_id)
            return new_id
        finally:
            self.c.execute('UNLOCK TABLES')

    def getContainerType(self, containerID, actualOnly = True):
        """Returns tuple:
            type string corresponding to a containerID
            project ID container belongs to (0 for root)"""
        # getting type of container
        sql = """SELECT ContainerType, TopParent FROM Containers
                WHERE CodeContainer = %d """ % containerID
        if actualOnly:
            sql += "  AND Status = 'Actual' "
        n = self.c.execute(sql)
        if not n:
            raise DBNotFoundException('Cannot find container with ID %d' % containerID)
        (a_type, pr_id) = self.c.fetchall()[0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return (a_type, pr_id)

    def getContainerName(self, containerID, actualOnly = True):
        """Returns tuple:
            (type string corresponding to a containerID,  Container name)"""
        # getting type of container
        sql = """SELECT ContainerType, ContainerName, TopParent FROM Containers
                WHERE CodeContainer = %d """ % containerID
        if actualOnly:
            sql += " AND Status = 'Actual' "
        n = self.c.execute(sql)
        if not n:
            raise DBNotFoundException('Cannot find container with ID %d' % containerID)
        (a_type, name, pr_id) = self.c.fetchall()[0]
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        return (a_type, name)

    def getSubContainersList(self, containerID, actualOnly = True):
        """Returns list of subordinate containers.
        Input:
            containerID - Parent container ID
        Output:
            list of tuples (ContainerID, type, name)"""
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        sql = """SELECT CodeContainer, ContainerType, ContainerName
                FROM Containers
                WHERE LinkUp = %d """ % containerID
        if actualOnly:
            sql += " AND Status = 'Actual' "
        n = self.c.execute(sql)
        return list(self.c.fetchall())

    def getSubContainersListByType(self, containerID, type):
        """Returns list of pairs: (container id, subordinate container name).
        Input:
            containerID - Parent container ID
            type - type identifier to select subcontainers
        Output:
            list of tuples (ContainerID, name)
        """
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        sql = """SELECT CodeContainer, ContainerName
                FROM Containers
                WHERE LinkUp = %d AND Status = "Actual"
                AND ContainerType = "%s" """ % (containerID, type)
        n = self.c.execute(sql)
        ans = list(self.c.fetchall())
        return ans
        
    def getSubContainersListWithCAttribute(self, containerID, a_name):
        """Returns list of subordinate containers and
        character attribute value.
        Input:
            containerID - Parent container ID
        Output:
            list of tuples (ContainerID, type, name, attribute_value)"""
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        # try to fetch all subcontainers - to know its types
        n = self.c.execute("""SELECT CodeContainer, ContainerType
                FROM Containers
                WHERE LinkUp = %d AND Status = "Actual" """ % containerID)
        c_list = self.c.fetchall()
        # now check if attribute a_name exists for all subcontainers
        checked_types = {}
        for cont in c_list:
            c_type = cont[1]
            if c_type in checked_types:
                continue
            m_d = self.MetaData[c_type]
            try:
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
            except KeyError:
                continue
#                raise DBException('Container type %s has no attribute %s' % (c_type, a_name))
            if form != 'C':
                continue
#                raise DBException('Container type %s has no character attribute %s' % (c_type, a_name))
            checked_types[c_type] = (form, sign_val, dim, md_id, ref_type, link_perms)
        # here we are sure all subcontainers have attribute
        # now - cycling over types and making queries
        out_list = []
        for cur_type in list(checked_types.keys()):
            (form, sign_val, dim, md_id, ref_type, link_perms) = checked_types[cur_type]
            sql_template = """SELECT CodeContainer, Containers.ContainerType, ContainerName, DataValue
            FROM Containers, MetaData, DataValuesC
            WHERE Containers.LinkUp = %d
            and Containers.Status = 'Actual'
            AND MetaData.CodeData = %d
            AND MetaData.KeyWord = "%s"
            AND DataValuesC.LinkMetaData = MetaData.CodeData
            AND DataValuesC.LinkContainer = Containers.CodeContainer
            AND DataValuesC.Status = "Actual"
            """ % (containerID, md_id, a_name)
            n = self.c.execute(sql_template)
            if n:
               out_list.extend( self.c.fetchall())
               
        return out_list

    def getSubContainersListWithPVAttribute(self, containerID, c_type, a_name):
        """Returns list of subordinate containers and
        vector of points attribute value.
        Input:
            containerID - Parent container ID
            c_type - type of container
        Output:
            list of tuples (ContainerName, X, Y, Z)"""
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        m_d = self.MetaData[c_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (c_type, a_name))
        if (form != 'P') and (dim != 1):
            raise DBException('Wrong attribute type for %s: %s is not a vector of points' % (c_type, a_name))
        sql_template = """SELECT  ContainerName, DataValueX, DataValueY, DataValueZ
            FROM Containers, MetaData, DataValuesP
            WHERE Containers.LinkUp = %d
            and Containers.Status = 'Actual'
            AND MetaData.CodeData = %d
            AND DataValuesP.LinkMetaData = MetaData.CodeData
            AND DataValuesP.LinkContainer = Containers.CodeContainer
            AND DataValuesP.Status = "Actual"
            """ % (containerID, md_id)
        # @IMPORTANT NOTE: We've removed ORDER BY CodeContainer, ValueIndex from
        # this SQL operator since it caused creating temporary table and sorting it,
        # which sometimes lead to error (on _very_ large datasets).
        # that means that the result can (theretically) be sorted illegaly.
        n = self.c.execute(sql_template)
        out_list = self.c.fetchall()
        return out_list


    def getSubContainersListWithCCAttributes(self, containerID, a_name1, a_name2):
        """Returns list of subordinate containers and
        2 character attribute value.
        Input:
            containerID - Parent container ID
            a_name1, a_name2 - names of attributes
        Output:
            list of tuples (ContainerID, type, name, attribute_value1, attribute_value2)"""
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        # try to fetch all subcontainers - to know its types
        n = self.c.execute("""SELECT CodeContainer, ContainerType
                FROM Containers
                WHERE LinkUp = %d AND Status = "Actual" """ % containerID)
        c_list = self.c.fetchall()
        # now check if attribute a_name exists for all subcontainers
        checked_types = {}
        for cont in c_list:
            c_type = cont[1]
            if c_type in checked_types:
                continue
            m_d = self.MetaData[c_type]
            try:
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name1]
            except KeyError:
                continue
            if form != 'C':
                continue
            ct1 = (form, sign_val, dim, md_id, ref_type, link_perms)
            try:
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name2]
            except KeyError:
                continue
            if form != 'C':
                continue
            ct2 = (form, sign_val, dim, md_id, ref_type, link_perms)
            checked_types[c_type] = (ct1, ct2)
        # here we are sure all subcontainers have attribute
        # now - cycling over types and making queries
        out_list = []
        for cur_type in list(checked_types.keys()):
            ((form1, sign_val1, dim1, md_id1, ref_type1, link_perms1),
             (form2, sign_val2, dim2, md_id2, ref_type2, link_perms2)) = checked_types[cur_type]
            sql_template = """SELECT CodeContainer, Containers.ContainerType, ContainerName, D1.DataValue, D2.DataValue
                FROM Containers, DataValuesC as D1, DataValuesC as D2
                WHERE Containers.LinkUp = %d
                and Containers.Status = 'Actual'
                AND D1.LinkMetaData = %d
                AND D1.LinkContainer = Containers.CodeContainer
                AND D1.Status = "Actual"
                AND D2.LinkMetaData = %d
                AND D2.LinkContainer = Containers.CodeContainer
                AND D2.Status = "Actual"
                """ % (containerID, md_id1, md_id2)
            n = self.c.execute(sql_template)
            if n:
               out_list.extend( self.c.fetchall())
               
        return out_list


    def getSubContainersListWithAttributes(self, containerID, c_type, a_list):
        """Returns list of subordinate containers names of given type and
        attributes values from the list a_list.
        Input:
            containerID - Parent container ID
            c_type - type of containers to get attributes for
            a_list  - lilst of names of attributes
        Output:
            list of tuples (ContainerID, name, value1, value2, ...)
        Note:
           Currently only IP combination of attributes supported
           Array ettributes are not supported
        """
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        m_d = self.MetaData[c_type]
        
        fields_list = []
        for a in a_list:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a]
            if dim != 0:
                DBException('Array containers not permited in getSubContainersListWithAttributes')
            fields_list.append((form, md_id))
        # collect sql operator
        sql_head = "SELECT CodeContainer, ContainerName "
        sql_from = " FROM Containers "
        sql_where = " WHERE Containers.LinkUp = %d AND Containers.Status = 'Actual' AND Containers.ContainerType = '%s' " % (containerID, c_type)

        ind = 1
        for f in fields_list:
            cn = "D%d" % ind
            an = "DataValues%s" % f[0]
            if f[0] == 'P':
               sql_head += ", %s.DataValueX, %s.DataValueY, %s.DataValueZ " %(cn, cn, cn)
            else:
                sql_head += ", %s.DataValue " % cn
            sql_from += ", %s AS %s " % (an, cn)
            sql_where += " AND %s.LinkMetaData = %d  AND %s.LinkContainer = Containers.CodeContainer AND  %s.Status = \"Actual\" " % (cn, f[1], cn, cn)
            ind += 1
        sql_template = sql_head + sql_from + sql_where
        n = self.c.execute(sql_template)
        out_list = self.c.fetchall()
        return out_list

    def getSubContainersListWithAttributesMissingAsNone(self, containerID, c_type, a_list):
        """Returns list of subordinate containers names of given type and
        attributes values from the list a_list. Missing attributes (or not defined attributes
        having NULL value in database) are returned as None.
        Input:
            containerID - Parent container ID
            c_type - type of containers to get attributes for
            a_list  - lilst of names of attributes
        Output:
            list of tuples (ContainerID, name, value1, value2, ...)
        Note:
           Currently only IP combination of attributes supported
           Array ettributes are not supported
        """
        # checking access permissions for project container belongs to
        self.getParentProject(containerID)
        m_d = self.MetaData[c_type]
        
        fields_list = []
        for a in a_list:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a]
            if dim != 0:
                DBException('Array containers not permited in getSubContainersListWithAttributes')
            fields_list.append((form, md_id))
        # collect sql operator
        sql_head = "SELECT CodeContainer, ContainerName "
        sql_from = " FROM Containers "
        sql_where = " WHERE Containers.LinkUp = %d AND Containers.ContainerType = \"%s\" AND Containers.Status = 'Actual' " % (containerID, c_type)
        ind = 1
        for f in fields_list:
            cn = "D%d" % ind
            an = "DataValues%s" % f[0]
            if f[0] == 'P':
               sql_head += ", %s.DataValueX, %s.DataValueY, %s.DataValueZ " %(cn, cn, cn)
            else:
                sql_head += ", %s.DataValue " % cn
            sql_from += " LEFT JOIN %s AS %s ON %s.LinkContainer = Containers.CodeContainer AND %s.LinkMetaData = %d AND  %s.Status = \"Actual\" " % (an, cn, cn, cn, f[1], cn)

            ind += 1
        sql_template = sql_head + sql_from + sql_where
        n = self.c.execute(sql_template)
        out_list = self.c.fetchall()
        return out_list


    def getSubContainersListWithProtectionOwner(self,  containerID, c_type):
        """Returns list of subordinate containers names of given type and other service attributes, including
        protection flag, owner name (may be None) and (not implemented yet) group name.
        Output:
           list of tuples (ContainerID, name, protection_flag, owner_name, None)
        """
        sql = "SELECT c.CodeContainer, c.ContainerName, c.isProtected, u.UserName, NULL AS UserGroup FROM Containers c  LEFT JOIN Users AS u ON c.ownerID = u.UserID WHERE c.LinkUp = %d AND c.ContainerType='%s' AND c.Status = 'Actual'" % (containerID, c_type)
        self.c.execute(sql)
        out_list = self.c.fetchall()
        return out_list

    def getSubSubContainersByNames(self, upper_id, mc_names, lc_name, lc_type = None):
        """Returns list of containers selected by name.
                  upper_container(upper_id)
                     --> middle_container(name in mc_names)
                        --> lower container(type == lc_type, lc_name)
        Input:
          upper_id - ID of upper container
          mc_names - list of names of middle containers. If empty - all containers
                     are retrieved
          lc_name - name of lower containers
          lc_type - additional constraint on type of selected containers
        Output:
          List of tuples: [(middle_name,  lower_name, type, id)]
        """
        # checking access permissions for project container belongs to
        self.getParentProject(upper_id)
        sql_template = """ SELECT c2.ContainerName, c1.ContainerName, c1.ContainerType, c1.CodeContainer
                       FROM  Containers as c1, Containers as c2
                       WHERE c2.LinkUp = %d
                       AND c2.Status = "Actual"
                       AND c2.CodeContainer = c1.LinkUp
                       AND c1.ContainerName = "%s"
                       AND c1.Status = "Actual"
                       """
        if mc_names:
            suppl_q1 = "AND c2.containerName in ("
            suppl_q2 = ','.join(['"' + s + '"' for s in mc_names]) # @TODO: Here we should use single quotes
            sql_template = sql_template + suppl_q1 + suppl_q2 + ')'
        sql = sql_template % (upper_id, lc_name)
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans

    def getSubSubContainersCA(self, upper_id, mc_names, lc_name, attr_name):
        """Returns list of containers with character attribute value.
                  upper_container(upper_id)
                     --> middle_container(name in mc_names)
                        --> lower container(type == lc_type, lc_name)
        Input:
          upper_id - ID of upper container
          mc_names - list of names of middle containers. If empty - all containers
                     are retrieved
          lc_name - name of lower containers
          attr_name - name of attribute to retrieve
        Output:
          List of tuples: [(middle_name, attr_value, lower_name)]
        """
        # checking access permissions for project container belongs to
        self.getParentProject(upper_id)
        sql_template = """ SELECT c2.ContainerName, v.DataValue, c1.ContainerName
                       FROM DataValuesC as v, Containers as c1, Containers as c2, MetaData
                       WHERE MetaData.KeyWord = "%s"
                       AND v.LinkMetaData=MetaData.CodeData
                       AND v.LinkContainer = c1.CodeContainer
                       AND c2.LinkUp = %d
                       AND c2.Status = "Actual"
                       AND c2.CodeContainer = c1.LinkUp
                       AND c1.ContainerName = "%s"
                       AND c1.Status = "Actual"
                       AND v.Status = "Actual"
                       """
        if mc_names:
            suppl_q1 = "AND c2.containerName in ("
            suppl_q2 = ','.join(['"' + s + '"' for s in mc_names])
            sql_template = sql_template + suppl_q1 + suppl_q2 + ')'
        sql = sql_template % (attr_name, upper_id, lc_name)
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans


    def getSubSubContainersByTypeCA(self, upper_id, mc_names, lc_type, attr_name):
        """Returns list of containers with character attribute value.
                  upper_container(upper_id)
                     --> middle_container(name in mc_names)
                        --> lower container(type == lc_type, lc_name)
        Input:
          upper_id - ID of upper container
          mc_names - list of names of middle containers. If empty - all containers
                     are retrieved
          lc_type - name of lower containers
          attr_name - name of attribute to retrieve
        Output:
          List of tuples: [(middle_name, lower_name, attr_value)]
        """
        # checking access permissions for project container belongs to
        self.getParentProject(upper_id)
        sql_template = """ SELECT c2.ContainerName, c1.ContainerName, v.DataValue
                       FROM DataValuesC as v, Containers as c1, Containers as c2, MetaData
                       WHERE MetaData.KeyWord = "%s"
                       AND v.LinkMetaData=MetaData.CodeData
                       AND v.LinkContainer = c1.CodeContainer
                       AND c2.LinkUp = %d
                       AND c2.Status = "Actual"
                       AND c2.CodeContainer = c1.LinkUp
                       AND c1.ContainerType = "%s"
                       AND c1.Status = "Actual"
                       AND v.Status = "Actual"
                       """
        if mc_names:
            suppl_q1 = "AND c2.containerName in ("
            suppl_q2 = ','.join(['"' + s + '"' for s in mc_names])
            sql_template = sql_template + suppl_q1 + suppl_q2 + ')'
        sql = sql_template % (attr_name, upper_id, lc_type)
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans

    def getSubSubContainersByTypeCAMissingAsNone(self, upper_id, mc_names, lc_type, attr_name):
        """Returns list of containers with character attribute value.
                  upper_container(upper_id)
                     --> middle_container(name in mc_names)
                        --> lower container(type == lc_type, lc_name)
           Acts the same way as getSubSubContainersByTypeCA but missing attributes are returned as None.
        Input:
          upper_id - ID of upper container
          mc_names - list of names of middle containers. If empty - all containers
                     are retrieved
          lc_type - name of lower containers
          attr_name - name of attribute to retrieve
        Output:
          List of tuples: [(middle_name, lower_name, attr_value)]
        """
        # checking access permissions for project container belongs to
        self.getParentProject(upper_id)
        m_d = self.MetaData[lc_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[attr_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (lc_type, attr_name))
        sql_template = """ SELECT DISTINCT c2.ContainerName, c1.ContainerName, v.DataValue
                       FROM  Containers as c2, MetaData, 
                       Containers as c1 LEFT JOIN DataValuesC AS v ON v.LinkContainer = c1.CodeContainer AND v.Status = "Actual" AND v.LinkMetaData=%d
                       WHERE c2.LinkUp = %d
                       AND c2.Status = "Actual"
                       AND c2.CodeContainer = c1.LinkUp
                       AND c1.ContainerType = "%s"
                       AND c1.Status = "Actual"
                       """
        if mc_names:
            suppl_q1 = "AND c2.containerName in ("
            suppl_q2 = ','.join(['"' + s + '"' for s in mc_names])
            sql_template = sql_template + suppl_q1 + suppl_q2 + ')'
        sql = sql_template % (md_id, upper_id, lc_type)
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans

    def getSubSubContainersNamesByType(self, upper_id, mc_names, lc_type):
        """Returns list of containers with names values of lower containers:
                  upper_container(upper_id)
                     --> middle_container(name in mc_names)
                        --> lower container(type == lc_type, lc_name)
        Input:
          upper_id - ID of upper container
          mc_names - list of names of middle containers. If empty - all containers
                     are retrieved
          lc_type - name of lower containers
        Output:
          List of tuples: [(middle_name, lower_name)]
        """
        # checking access permissions for project container belongs to
        self.getParentProject(upper_id)
        sql_template = """ SELECT c2.ContainerName, c1.ContainerName
                       FROM Containers as c1, Containers as c2
                       WHERE c2.LinkUp = %d
                       AND c2.Status = "Actual"
                       AND c2.CodeContainer = c1.LinkUp
                       AND c1.ContainerType = "%s"
                       AND c1.Status = "Actual"
                       """
        if mc_names:
            suppl_q1 = "AND c2.containerName in ("
            suppl_q2 = ','.join(['"' + s + '"' for s in mc_names])
            sql_template = sql_template + suppl_q1 + suppl_q2 + ')'
        sql = sql_template % (upper_id, lc_type)
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans

    def getPairSubWithAttrsCCCUndefsAsNone(self, upper_id, mc_type, lc_type, a1_name, a2_name, lc_name, p_name):
        """Returns list of lines corresponding to properties of pairs of containers :
                  upper_container(upper_id)
                     --> middle_container(type is mc_type)
                        --> lower container(type == lc_type, name is lc_name)
        Input:
        
        Output:
          List of tuples: [(middle_name, attribute1, attribute2, property_value)]
        """
        # checking access permissions for project container belongs to
        self.getParentProject(upper_id)
        m_d_m = self.MetaData[mc_type]
        m_d_l = self.MetaData[lc_type]
        lmd_a1 = m_d_m[a1_name][3] # getting CodeData corresponding to a1_name
        lmd_a2 = m_d_m[a2_name][3] # 
        lmd_p = m_d_l[p_name][3] # 
        
        sql_template = """ SELECT cp.ContainerName, a1.DataValue, a2.DataValue, pv.DataValue
                      FROM  Containers cp  
                      LEFT JOIN Containers c ON c.LinkUp = cp.CodeContainer AND   c.ContainerType = '%s' AND   c.Status = 'Actual' AND c.ContainerName = '%s'
                      LEFT JOIN DataValuesC AS a1 ON a1.LinkContainer = cp.CodeContainer AND a1.Status = 'Actual' AND a1.LinkMetaData = %d
                      LEFT JOIN DataValuesC AS a2 ON a2.LinkContainer = cp.CodeContainer AND a2.Status = 'Actual' AND a2.LinkMetaData = %d
                      LEFT JOIN  DataValuesC pv ON pv.LinkContainer = c.CodeContainer AND pv.Status = 'Actual' AND pv.LinkMetaData = %d
                      WHERE  cp.LinkUp = %d
                      AND    cp.ContainerType = '%s'
                      AND    cp.Status = 'Actual'
                       """
        sql = sql_template % (lc_type, lc_name, lmd_a1, lmd_a2, lmd_p, upper_id, mc_type)
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans
        
    def getDistinctNameAttrByType(self, project_name, c_type, attrs_list):
        """Return list of tuples, consisting of names of containers of 
        given type (c_type) and additional attributes whose names
        are specified  by attrs_list.
        """
        pr_id = self.getProjectByName(project_name)
        sql_template = """
                 select distinct c.ContainerName, v.DataValue
                 from Containers as c, DataValuesC as v, MetaData where
                 c.ContainerType='%s' and 
                 c.Status='Actual' 
                 and c.TopParent = %d AND
                 MetaData.KeyWord = '%s'  AND 
                 v.LinkMetaData=MetaData.CodeData AND 
                 v.LinkContainer = c.CodeContainer AND
                 v.Status = 'Actual'
        """
        sql = sql_template % (c_type, pr_id, attrs_list[0])
        n = self.c.execute(sql.encode('utf8'))
        if n:
           ans = list(self.c.fetchall())
        else:
            ans = []
        return ans

    def setContainerAttributes(self, containerID, attr):
        """Sets all valid attributes for container
        Input:
            container_id - ID of container attributes are set for
            attr - dictionary of attributes.
                Format of dictionary is as follows:
                {name: value}
        Output:
            Number of attributes set"""

        self.auth.assertNotProtected(containerID)
        (c_type, pr_id) = self.getContainerType(containerID)
        # extract all metadata about container type
        m_d = self.MetaData[c_type]
        # Now - check if all attribute names are valid
        n_changed = 0
        for nm in list(attr.keys()):
            self.setContainerSingleAttribute(containerID, nm, attr[nm])
            n_changed += 1
        return n_changed


    def renameChildContainers(self, upper_containers_list, old_name, new_name):
        """Renames containers with parents IDs in
        containers_list changing names from old_name to new_name.
        Input:
            upper_containers_list - list of IDs of parent containers
            old_name - old name
            new_name - new name
        Output:
            new_name if success or old_name if there are duplicate names
        """
        # success if upper_containers_list is empty:
        if not upper_containers_list:
            return new_name
        # check if all containers belong to the same project
        common_prid = None
        for id in upper_containers_list:
            (c_type, pr_id) = self.getContainerType(id)
            if common_prid is None:
                common_prid = pr_id
            else:
                if pr_id != common_prid:
                    raise DBException('Containers ids %d and %d belong to different projects' % (upper_containers_list[0], id))
        # check permissions
        self.auth.checkPermissions(pr_id, Authorities.UPDT_ATTR)

        # Strip and check the validity of name
        new_name = new_name.strip()
        if not new_name:
            raise DBException('Renaming container to empty name attempted (old name: %s' % old_name)

        try:
            self.c.execute('LOCK TABLES Containers WRITE, ChangeLog WRITE')
            # Check that there is no containers among siblings with new_name
            sql_tmpl = """SELECT CodeContainer
                    FROM Containers
                    WHERE LinkUp = %d
                    AND ContainerName = "%s"
                    AND Status = "Actual" """
            for id in upper_containers_list:
                n = self.c.execute(sql_tmpl % (id, new_name))
                if n == 0:
                    continue
                # Failed with new_name: there are duplicates
                return old_name
            # here - starting renaming
            sql_tmpl = ''' UPDATE Containers
                                   SET ContainerName="%s"
                                   WHERE LinkUp=%d
                                   AND ContainerName="%s"
                                   AND Status = "Actual" '''
            sql_ids = """ SELECT CodeContainer FROM Containers
                                   WHERE LinkUp=%d
                                   AND ContainerName='%s'
                                   AND Status = 'Actual' """
            for id in upper_containers_list:
                self.c.execute(sql_ids % (id, old_name))
                renamedIDs = self.c.fetchall()
                self.c.execute(sql_tmpl % (new_name, id, old_name))
                for nContainer in renamedIDs:
                    self.auth.addLog('Containers', nContainer[0], 'Rename')
            return new_name
        finally:
            self.c.execute('UNLOCK TABLES')
    
    def setContainerSingleAttribute(self, container_id, a_name, a_value = None):
        """Set single container attribute
        Input:
            continer_id - identifier of container to set attribute value
            a_name - name of the attribute
            a_value - value of the attribute. Default is None which is not suitable
                     for some types of data. None stands for default value, supported
                     only for datimes.
        Output:
            >0 if success
        Exceptions:
            - may raise DBException if there is no such attribute in
                container type metadata
            - conversion exceptions may be raised"""
        self.auth.assertNotProtected(container_id)
        (c_type, pr_id) = self.getContainerType(container_id)
        # extract all metadata about container type
        m_d = self.MetaData[c_type]
        a_desc = m_d[a_name]
        # looking at the dimension:
        if a_desc[2] != 0 and not (a_desc[2] is None):
            # this is an array attribute
            return self.setContainerArrayAttribute(container_id, a_name, a_value)
        perm_flags = self.auth.getPermissions(pr_id)
        n_changed = 0
        replaced_values = []
        if a_desc[0] in ['R', 'X']:
            # Locking both Containers and data values in case of references
            self.c.execute('LOCK TABLES Containers WRITE, DataValues%s WRITE' % a_desc[0])
        else:
            self.c.execute('LOCK TABLES DataValues%s WRITE' % a_desc[0])
        #a_desc[3] - line number in MetaData
        try:
            n = self.c.execute("""SELECT CodeValue FROM DataValues%s
                WHERE LinkContainer = %d
                AND LinkMetaData = %d
                AND Status = "Actual" """ % (a_desc[0], container_id, a_desc[3]))
            replaced_values = self.c.fetchall()
            # now - mark all replaced values as deleted
            if n:
                # updating attributes values
                try:
                    self.auth.checkPermissions(pr_id, Authorities.UPDT_ATTR, perm_flags)
                except DBAuthoritiesException as ex:
                    raise ex
                sql = """UPDATE DataValues%s
                        SET Status = "Deleted"
                        WHERE LinkContainer = %d
                        AND LinkMetaData = %d
                        AND Status = "Actual" """ % (a_desc[0], container_id, a_desc[3])
                self.c.execute(sql.encode('utf-8'))
            else:
                # creating new attribute - lets check it
                try:
                    self.auth.checkPermissions(pr_id, Authorities.CREATE_ATTR, perm_flags)
                except DBAuthoritiesException as ex:
                    raise ex
            if a_desc[0] == 'C':
                # Store string value
                sql = """INSERT DataValuesC (LinkContainer, LinkMetaData, DataValue)
                    VALUES (%d, %d, "%s") """ % (container_id, a_desc[3], a_value)
                self.c.execute(sql.encode('utf-8'))
                n_changed += 1
            elif a_desc[0] == 'T':
                # Store time value
                if a_value is None:
                    self.c.execute("""INSERT DataValuesT (LinkContainer, LinkMetaData, DataValue)
                         VALUES (%d, %d, NULL) """ % (container_id, a_desc[3]))
                else:
                    self.c.execute("""INSERT DataValuesT (LinkContainer, LinkMetaData, DataValue)
                         VALUES (%d, %d, "%s") """ % (container_id, a_desc[3], a_value))
                n_changed += 1
            elif a_desc[0] == 'I':
                # Store integer value
                try:
                    a_value = int(a_value)
                except ValueError:
                    raise DBException('Attribute value error in setContainerSingleAttribute')
                self.c.execute("""INSERT DataValuesI (LinkContainer, LinkMetaData, DataValue)
                    VALUES (%d, %d, %d) """ % (container_id, a_desc[3],  a_value))
                n_changed += 1
            elif a_desc[0] == 'F':
                # store float value
                try:
                    val = float(a_value)
                    val = val * pow(10., a_desc[1])
                    val = int(val)
                except ValueError:
                    raise DBException('Attribute value error in setContainerSingleAttribute')
                self.c.execute("""INSERT DataValuesF (LinkContainer, LinkMetaData, DataValue)
                    VALUES (%d, %d, %d) """ % (container_id, a_desc[3],  val))
                n_changed += 1
            elif a_desc[0] == 'D':
                # Store double value
                obj_type = 'DataValuesD'
                try:
                    a_value = float(a_value)
                except ValueError:
                    raise DBException('Attribute value error in setContainerSingleAttribute')
                self.c.execute("""INSERT DataValuesD (LinkContainer, LinkMetaData, DataValue)
                    VALUES (%d, %d, %g) """ % (container_id, a_desc[3],  a_value))
                n_changed += 1
            elif a_desc[0] == 'R':
                # check if referenced container type is valid
                a_value = int(a_value)
                n = self.c.execute("""SELECT ContainerType, TopParent FROM Containers
                    WHERE CodeContainer = %d AND Status = "Actual" """ % a_value)
                if n == 0:
                    raise DBNotFoundException('Referenced container %d does not exist' % a_value)
                (ref_type, ref_project) = self.c.fetchall()[0]
                if not (a_desc[4] is None) and (ref_type != a_desc[4]):
                    raise DBException('Type %s not allowed to be referenced by %s' % (ref_type, a_name))
                if (a_desc[5] == 'OwnProject') and (ref_project != pr_id):
                    raise DBException('Reference permission violation: ref. OwnProject references from %d to %d for container %d' % (ref_project, pr_id, container_id))
                # Store reference value
                obj_type = 'DataValuesR'
                self.c.execute("""INSERT DataValuesR (LinkContainer, LinkMetaData, DataValue)
                    VALUES (%d, %d, %d) """ % (container_id, a_desc[3],  a_value))
                n_changed += 1
            elif a_desc[0] == 'X':
                # store value (D, ref)
                # check if referenced container type is valid
                a_valuer = int(a_value[1])
                a_valued = float(a_value[0])
                n = self.c.execute("""SELECT ContainerType, TopParent FROM Containers
                    WHERE CodeContainer = %d AND Status = "Actual" """ % a_valuer)
                if n == 0:
                    raise DBNotFoundException('Referenced container %d does not exist' % a_valuer)
                (ref_type, ref_project) = self.c.fetchall()[0]
                if not (a_desc[4] is None) and (ref_type != a_desc[4]):
                    raise DBException('Type %s not allowed to be referenced by %s' % (ref_type, a_name))
                if (a_desc[5] == 'OwnProject') and (ref_project != pr_id):
                    raise DBException('Reference permission violation: ref. OwnProject references from %d to %d for container %d' % (ref_project, pr_id, container_id))
                # Store reference value
                obj_type = 'DataValuesX'
                self.c.execute("""INSERT DataValuesR (LinkContainer, LinkMetaData, DataValueD, DataValueR)
                    VALUES (%d, %d, %f, %d) """ % (container_id, a_desc[3],  a_valued, a_valuer) )
                n_changed += 1
            elif a_desc[0] == 'P':
                # Store (x, y, z) value
                try:
                    (a_valuex, a_valuey, a_valuez) = a_value
                    a_valuex = float(a_valuex)
                    a_valuey = float(a_valuey)
                    a_valuez = float(a_valuez)
                except Exception as ex:
                    raise DBException('Attribute value error in setContainerSingleAttribute')
                obj_type = 'DataValuesP'
                self.c.execute("""INSERT DataValuesP (LinkContainer, LinkMetaData, DataValueX, DataValueY, DataValueZ)
                    VALUES (%d, %d, %f, %f, %f) """ % (container_id, a_desc[3],  a_valuex, a_valuey, a_valuez))
                n_changed += 1
            else:
                raise DBException('Attribute type %s not supported in setContainerSingleAttribute' % a_desc[0])
            self.c.execute("SELECT LAST_INSERT_ID()")
            attr_id = self.c.fetchall()[0][0]
        finally:
            self.c.execute("""UNLOCK TABLES""")   # Some exception was raised!
        self.auth.addLog('DataValues' + a_desc[0], attr_id)
        if replaced_values:
            pass
        for v_id in replaced_values:
            self.auth.addLog('DataValues' + a_desc[0], v_id[0], 'Delete')
        return n_changed

    def setContainerArrayAttribute(self, containerID, a_name, value):
        """Sets attribute value when it is array attribute"""
        self.auth.assertNotProtected(containerID)
        (c_type, pr_id) = self.getContainerType(containerID)
        perm_flags = self.auth.getPermissions(pr_id)
        m_d = self.MetaData[c_type]
        (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        if dim == 0:
            raise DBException('Non - array attribute value requested for %d, array attr. %s' % (containerID, a_name))
        if dim > 1:
            raise DBException('Multidimension attributes not implemented. ID %d, array attr. %s' % (containerID, a_name))

        # Doing preparative work
        if form in ['R', 'X']:
            # Locking both Containers and data values in case of references
            self.c.execute('LOCK TABLES Containers WRITE, DataValues%s WRITE' % form)
        else:
            self.c.execute('LOCK TABLES DataValues%s WRITE' % form)
        #a_desc[3] - line number in MetaData
        try:
            n = self.c.execute("""SELECT CodeValue FROM DataValues%s
                WHERE LinkContainer = %d
                AND LinkMetaData = %d
                AND Status = "Actual" """ % (form, containerID, md_id))
            replaced_values = self.c.fetchall()
            # now - mark all replaced values as deleted
            if n:
                # trying to update attribute values
                try:
                    self.auth.checkPermissions(pr_id, Authorities.UPDT_ATTR, perm_flags)
                except DBAuthoritiesException as ex:
                    raise ex
                self.c.execute("""UPDATE DataValues%s
                        SET Status = "Deleted"
                        WHERE LinkContainer = %d
                        AND LinkMetaData = %d
                        AND Status = "Actual" """ % (form, containerID, md_id))
            else:
                try:
                    self.auth.checkPermissions(pr_id, Authorities.CREATE_ATTR, perm_flags)
                except DBAuthoritiesException as ex:
                    raise ex
            val = None
            ind = 0
            if form == 'C':
                # chars
                for val in value:
                    self.c.execute("""INSERT DataValuesC (LinkContainer, LinkMetaData, ValueIndex, DataValue)
                        VALUES (%d, %d, %d, "%s") """ % (containerID, md_id, ind, val))
                    ind += 1
            elif form == 'T':
                # chars
                for val in value:
                    self.c.execute("""INSERT DataValuesT (LinkContainer, LinkMetaData, ValueIndex, DataValue)
                        VALUES (%d, %d, %d, "%s") """ % (containerID, md_id, ind, val))
                    ind += 1
            elif form == 'I':
                # ints
                for val in value:
                    val = int(val)
                    self.c.execute("""INSERT DataValuesI (LinkContainer, LinkMetaData, ValueIndex, DataValue)
                        VALUES (%d, %d, %d, %d) """ % (containerID, md_id, ind, val))
                    ind += 1
            elif form == 'F':
                # fixed
                for val in value:
                    val = val * pow(10., sign_val)
                    val = int(val)
                    self.c.execute("""INSERT DataValuesF (LinkContainer, LinkMetaData, ValueIndex, DataValue)
                        VALUES (%d, %d, %d, %d) """ % (containerID, md_id, ind, val))
                    ind += 1
            elif form == 'D':
                # doubles
                for val in value:
                    val = float(val)
                    self.c.execute("""INSERT DataValuesD (LinkContainer, LinkMetaData, ValueIndex, DataValue)
                        VALUES (%d, %d, %d, %f) """ % (containerID, md_id, ind, val))
                    ind += 1
            elif form == 'P':
                # points
                for val in value:
                    try:
                        valx, valy, valz = val
                        valx, valy, valz = (float(valx), float(valy), float(valz))
                    except ValueError:
                        raise DBException('Invalid argument')
                    self.c.execute("""INSERT DataValuesP (LinkContainer, LinkMetaData, ValueIndex, DataValueX, DataValueY, DataValueZ)
                        VALUES (%d, %d, %d, %f, %f, %f) """ % (containerID, md_id, ind, valx, valy, valz))
                    ind += 1
            elif form == 'R':
                for val in value:
                    a_value = int(val)
                    n = self.c.execute("""SELECT ContainerType, TopParent FROM Containers
                        WHERE CodeContainer = %d AND Status = "Actual" """ % a_value)
                    if n == 0:
                        raise DBNotFoundException('Referenced container %d does not exist' % a_value)
                    (cur_ref_type, ref_project) = self.c.fetchall()[0]
                    if  not (ref_type is None) and (cur_ref_type != ref_type):
                        raise DBException('Type %s not allowed to be referenced by %s' % (ref_type, a_name))
                    if (link_perms == 'OwnProject') and (ref_project != pr_id):
                        raise DBException('Reference permission violation: ref. OwnProject references from %d to %d for container %d' % (ref_project, pr_id, container_id))
                    # Store reference value
                    obj_type = 'DataValuesR'
                    self.c.execute("""INSERT DataValuesR (LinkContainer, LinkMetaData, ValueIndex, DataValue)
                        VALUES (%d, %d, %d, %d) """ % (containerID, md_id, ind, a_value))
                    ind += 1
            elif form == 'X':
                for val in value:
                    a_valuer = int(val[1])
                    a_valued = float(val[0])
                    n = self.c.execute("""SELECT ContainerType, TopParent FROM Containers
                        WHERE CodeContainer = %d AND Status = "Actual" """ % a_valuer)
                    if n == 0:
                        raise DBNotFoundException('Referenced container %d does not exist' % a_valuer)
                    (cur_ref_type, ref_project) = self.c.fetchall()[0]
                    if  not (ref_type is None) and (cur_ref_type != ref_type):
                        raise DBException('Type %s not allowed to be referenced by %s' % (ref_type, a_name))
                    if (link_perms == 'OwnProject') and (ref_project != pr_id):
                        raise DBException('Reference permission violation: ref. OwnProject references from %d to %d for container %d' % (ref_project, pr_id, container_id))
                    # Store reference value
                    obj_type = 'DataValuesX'
                    self.c.execute("""INSERT DataValuesX (LinkContainer, LinkMetaData, ValueIndex, DataValueD, DataValueR)
                        VALUES (%d, %d, %d, %f, %d) """ % (containerID, md_id, ind, a_valued, a_valuer))
                    ind += 1
            else:
                raise DBException('Attribute type %s not supported in setContainerArrayAttribute' % form)

            self.c.execute("SELECT LAST_INSERT_ID()")
            attr_id = self.c.fetchall()[0][0]
        finally:
            self.c.execute("""UNLOCK TABLES""")
        self.auth.addLog('DataValues' + form, attr_id)
        if replaced_values:
            pass
        for v_id in replaced_values:
            self.auth.addLog('DataValues' + form, v_id[0], 'Delete')
        return ind

    def getContainerSingleAttributeWithDefault(self, containerID, a_name, default_val):
        """Returns default value if there is no associated value.
        """
        try:
            val = self.getContainerSingleAttribute(containerID, a_name)
        except DBException:
            val = default_val
        return val

    def getContainerSingleAttribute(self, containerID, a_name, a_type=None, m_d=None):
        """Returns value of attribute a_name of container with ID containerID
        Raises KeyError if there is no such attribute or DBException if
        attribute has no associated value."""
        if a_type == None:
            (a_type, pr_id) = self.getContainerType(containerID)
        if m_d == None:
            m_d = self.MetaData[a_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (a_type, a_name))
        if dim:
            return self.getContainerArrayAttribute(containerID, a_name)
        val = None
        sql_template = """SELECT DataValue FROM Containers, MetaData, %s
                WHERE Containers.CodeContainer = %d
                AND MetaData.CodeData = %d
                AND MetaData.KeyWord = "%s"
                AND %s.LinkMetaData = MetaData.CodeData
                AND %s.LinkContainer = Containers.CodeContainer
                AND %s.Status = "Actual"
                """
        # (valType, containerID, md_id, a_name, valType, valType, valType)
        if form == 'C':
            # chars
            valType = 'DataValuesC'
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = self.c.fetchall()[0][0]
        elif form == 'T':
            # Time
            valType = 'DataValuesT'
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                ## val = str(self.c.fetchall()[0][0])
                val = self.c.fetchall()[0][0]  # !TODO: check if that works
        elif form == 'I':
            # ints
            valType = 'DataValuesI'
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = int(self.c.fetchall()[0][0])
        elif form == 'F':
            # fixed
            valType = 'DataValuesF'
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = float(self.c.fetchall()[0][0])
                val = val / pow(10., sign_val)
        elif form == 'D':
            # double
            valType = 'DataValuesD'
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = self.c.fetchall()[0][0]
        elif form == 'R':
            # Reference
            valType = 'DataValuesR'
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = self.c.fetchall()[0][0]
        elif form == 'X':
            # Reference and double value
            valType = 'DataValuesX'
            sql_template = """SELECT DataValueD, DataValueR
                    FROM Containers, MetaData, %s
                    WHERE Containers.CodeContainer = %d
                    AND MetaData.CodeData = %d
                    AND MetaData.KeyWord = "%s"
                    AND %s.LinkMetaData = MetaData.CodeData
                    AND %s.LinkContainer = Containers.CodeContainer
                    AND %s.Status = "Actual"
                    """
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = self.c.fetchall()[0]
        elif form == 'P':
            # point: (x, y, z)
            valType = 'DataValuesP'
            sql_template = """SELECT DataValueX, DataValueY, DataValueZ
                    FROM Containers, MetaData, %s
                    WHERE Containers.CodeContainer = %d
                    AND MetaData.CodeData = %d
                    AND MetaData.KeyWord = "%s"
                    AND %s.LinkMetaData = MetaData.CodeData
                    AND %s.LinkContainer = Containers.CodeContainer
                    AND %s.Status = "Actual"
                    """
            # (valType, containerID, md_id, a_name, valType, valType, valType)
            n = self.c.execute(sql_template % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n > 1:
                raise DBException('More than one attribute values for %d %s' % (containerID, a_name))
            if n == 1:
                val = self.c.fetchall()[0]
        else:
            raise DBException('Usupported attribute type %s for container %d' % (form, containerID))
        if val is None:
            raise DBException('Container %d: value not set for %s' % (containerID, a_name))
        return val

    def getContainerArrayAttribute(self, containerID, a_name, a_type=None, m_d=None):
        """Gets attribute value when attribute is an array.
        Raises DBException when there is no value associated with attribute.
        Returns list of values"""
        if a_type == None:
            (a_type, pr_id) = self.getContainerType(containerID)
        if m_d == None:
            m_d = self.MetaData[a_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (a_type, a_name))
        if dim == 0:
            raise DBException('Non - array attribute value requested for %d, array attr. %s' % (containerID, a_name))
        if dim > 1:
            raise DBException('Multidimension attributes not implemented. ID %d, array attr. %s' % (containerID, a_name))
        sql_templ = """SELECT ValueIndex, DataValue FROM Containers, MetaData, %s
                WHERE Containers.CodeContainer = %d
                AND MetaData.CodeData = %d
                AND MetaData.KeyWord = "%s"
                AND %s.LinkMetaData = MetaData.CodeData
                AND %s.LinkContainer = Containers.CodeContainer
                AND %s.Status = "Actual"
                ORDER BY ValueIndex
                """
        values = []
        if form == 'C':
            # chars
            valType = 'DataValuesC'
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = ind_and_val[1]
                    values.append(val)
        elif form == 'I':
            # ints
            valType = 'DataValuesI'
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = int(ind_and_val[1])
                    values.append(val)
        elif form == 'F':
            # fixed
            valType = 'DataValuesF'
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = float(ind_and_val[1])
                    val = val / pow(10., sign_val)
                    values.append(val)
        elif form == 'D':
            # doubles
            valType = 'DataValuesD'
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = ind_and_val[1]
                    values.append(val)
        elif form == 'R':
            # reference
            valType = 'DataValuesR'
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = ind_and_val[1]
                    values.append(val)
        elif form == 'X':
            # reference
            valType = 'DataValuesX'
            sql_templ = """SELECT ValueIndex, DataValueD, DataValueR
                    FROM Containers, MetaData, %s
                    WHERE Containers.CodeContainer = %d
                    AND MetaData.CodeData = %d
                    AND MetaData.KeyWord = "%s"
                    AND %s.LinkMetaData = MetaData.CodeData
                    AND %s.LinkContainer = Containers.CodeContainer
                    AND %s.Status = "Actual"
                    ORDER BY ValueIndex
                    """
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = ind_and_val[1:3]
                    values.append(tuple(val))
        elif form == 'P':
            # points array
            valType = 'DataValuesP'
            sql_templ = """SELECT ValueIndex, DataValueX, DataValueY, DataValueZ
                    FROM Containers, MetaData, %s
                    WHERE Containers.CodeContainer = %d
                    AND MetaData.CodeData = %d
                    AND MetaData.KeyWord = "%s"
                    AND %s.LinkMetaData = MetaData.CodeData
                    AND %s.LinkContainer = Containers.CodeContainer
                    AND %s.Status = "Actual"
                    ORDER BY ValueIndex
                    """
            n = self.c.execute(sql_templ % (valType, containerID, md_id, a_name, valType, valType, valType))
            if n:
                vallist = self.c.fetchall()
                for ind_and_val in vallist:
                    val = ind_and_val[1:4]
                    values.append(tuple(val))
        else:
            raise DBException('Usupported attribute type %s for container %d' % (form, containerID))
        if not values:
            raise DBException('Container %d: value not set for %s' % (containerID, a_name))
        return values

    def getContainerArrayAttributeByIdAndIndex(self, containerID, a_name, attr_id, index):
        """

        :param containerID:
        :param a_name:
        :param attr_id: ID of attribute corresponding to the maximum index
        :param index: Maximum index in the array
        :return: [(val), (val), ...] or [(d, r), (d, r), ...] in the X case
        """
        (a_type, pr_id) = self.getContainerType(containerID)
        m_d = self.MetaData[a_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (a_type, a_name))
        if dim == 0:
            raise DBException('Non - array attribute value requested for %d, array attr. %s' % (containerID, a_name))
        if dim > 1:
            raise DBException('Multidimension attributes not implemented. ID %d, array attr. %s' % (containerID, a_name))
        assert form == 'X', 'Only DataValuesX are currently supported by getContainerArrayAttributeByIdAndIndex'
        min_attr_id = attr_id - index
        # sql = "select * from DataValuesX where CodeValue<=3025375 and LinkContainer=13430386 and CodeValue>=(3025375-2);"
        sql = """SELECT  d.DataValueD, d.DataValueR
                    FROM DataValuesX d
                    WHERE
                    d.LinkMetaData = %d
                    AND d.LinkContainer = %d
		    AND d.CodeValue >= %d
		    AND d.CodeValue <= %d
                    ORDER BY d.ValueIndex;
        """ % (md_id, containerID, min_attr_id, attr_id)
        self.c.execute(sql)
        return self.c.fetchall()

    def getContainerAttributes(self, containerID):
        """Returns dictionary of values of container attributes"""
        (a_type, pr_id) = self.getContainerType(containerID)
        m_d = self.MetaData[a_type]
        vals = {}
        for a_name in list(m_d.keys()):
            try:
                val = self.getContainerSingleAttribute(containerID, a_name, a_type, m_d)
            except DBException:
                continue
            vals[a_name] = val
        return vals

    def getDistinctNamesByType(self, project_name, c_type):
        """Returns all distinct names for containers having
        type c_type.
        Input:
          project_name - name of project
          c_type = type of container
        Output:
          Unsorted list of names (strings).
        """
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        n = self.c.execute("""SELECT DISTINCT ContainerName FROM Containers
        WHERE TopParent=%d
        AND Status = "Actual"
        AND ContainerType = "%s" """ % (pr_id, c_type) )
        if n == 0:
            return []
        else:
#            l = [unicode(p[0], 'utf8') for p in self.c.fetchall()]
            l = [p[0] for p in self.c.fetchall()]
        return l
 
    def countSubContainersByType(self, containerID, parent_type, child_types, output_zeros = False):
        """For every container having type parent_type count number of containers with type child_type
        """
        if output_zeros:
            sql_templ = """SELECT p.CodeContainer, p.ContainerName, COUNT(c.ContainerName) FROM Containers as p 
                           LEFT JOIN Containers AS c ON p.CodeContainer=c.LinkUp AND c.ContainerType IN (%s) AND c.Status='Actual' 
                           WHERE p.LinkUp=%d AND p.Status='Actual' AND p.ContainerType='%s' 
                           GROUP BY c.LinkUp, p.CodeContainer"""
        else:
            sql_templ = """SELECT p.CodeContainer, p.ContainerName, COUNT(c.ContainerName) FROM Containers as p 
                           JOIN Containers AS c ON p.CodeContainer=c.LinkUp AND c.ContainerType IN (%s) AND c.Status='Actual' 
                           WHERE p.LinkUp=%d AND p.Status='Actual' AND p.ContainerType='%s' 
                           GROUP BY c.LinkUp, p.CodeContainer"""
            
        assert (type(child_types) == list), 'Wrong argument type for countSubContainersByType, must be list'
        child_str = ", ".join(["'" + s + "'" for s in child_types])
        sql = sql_templ % (child_str, containerID, parent_type)
        n = self.c.execute(sql.encode('utf8'))
        l = self.c.fetchall()
        return l
       

    def getDistinctNamesByTypeSubordinateToParents(self, project_name, c_type, parents):
        """Returns all distinct names for containers having
        type c_type and subordinate to given parents.
        Input:
          project_name - name of project
          c_type = type of container
          parents = list of names of parents
        Output:
          Unsorted list of names (strings).
        """
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        in_part = ""
        if parents:
            in_part = ' AND cp.ContainerName IN ('
            for c in parents[:-1]:
                in_part += '"' + c + '",'
            in_part += '"' + parents[-1] + '")'
        sql = """SELECT DISTINCT c.ContainerName FROM Containers as c,  Containers as cp 
        WHERE c.TopParent=%d
        AND cp.TopParent=%d
        AND c.Status = "Actual"
        AND c.ContainerType = "%s" 
        AND c.LinkUp = cp.CodeContainer
        %s"""  % (pr_id, pr_id, c_type, in_part) 
        n = self.c.execute(sql.encode('utf8'))
        if n == 0:
            return []
        else:
            l = [p[0] for p in self.c.fetchall()]
        return l
        
    def getAttributeOfSubcontainersByNameOfContainersWhereParentsInSet(self, pr_id, parents, c_type, l_name, a_name):
        """Returns values of attribute of containers having type c_type where
          pr_id <- c3(name in parents) <- c2 <- c1(name == l_name)
        Input:
          pr_id - id of project
          c_type = type of container
          parents = list of names of parents
          l_name = name of lower container
          a_name = name of attribute of lower container to fetch 
        Output:
          List of tuples: [(upper_parent_name, middle_container_id, middle_container_name, attribute_value), ...]
        """
        in_part = ""
        if parents:
            in_part = ' AND c3.ContainerName IN (\'' + '\', \''.join(parents) + '\')'
        sql = """
                 SELECT c3.ContainerName, c2.CodeContainer, c2.ContainerName,  a.DataValue
                       FROM Containers as c1, Containers as c2, Containers as c3, DataValuesC as a, MetaData as m
                       WHERE c3.status='Actual'
                       AND c3.LinkUp = %d 
                       AND c2.LinkUp = c3.CodeContainer
                       AND c2.Status = 'Actual'
                       AND c2.CodeContainer = c1.LinkUp
                       AND c1.ContainerType = '%s'
                       AND c1.Status = 'Actual'
                       AND c1.ContainerName = '%s'
                       AND a.LinkContainer = c1.CodeContainer
                       AND a.LinkMetaData= m.CodeData 
                       AND a.Status = 'Actual'
                       AND m.ContainerType='%s' 
                       AND m.KeyWord = '%s'

        %s"""  % (pr_id, c_type, l_name, c_type, a_name, in_part) 
        n = self.c.execute(sql.encode('utf8'))
        if n == 0:
            return []
        else:
            l = self.c.fetchall()
        return l
        

    def getReferencingObjects(self, containerID, type = None):
        """Returns containers referencing to containerID.
        Output may be limited to types of containers type.
        """
        self.getParentProject(containerID)
##        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        # !!!efr - we should select actual containers only
        if not type:
            sql = """SELECT LinkContainer FROM DataValuesR
                     WHERE  DataValue = %d
                     AND DataValuesR.Status = 'Actual'
            """ % containerID
        else:
            sql = """SELECT DISTINCT CodeContainer FROM Containers, DataValuesR
            WHERE DataValuesR.DataValue=%d
            AND DataValuesR.LinkContainer=CodeContainer
            AND DataValuesR.Status = 'Actual'
            AND ContainerType='%s'
            AND Containers.Status = 'Actual'
            """ % (containerID, type)
        n = self.c.execute(sql)
        res = self.c.fetchall()
        ans = []
        for i in range(n):
            ans.append(res[i][0])
        return ans

    def getReferencingObjectsByAttribute(self, containerID, container_type, attr_name):
        """Returns containers referencing to containerID.
        Output may be limited to types of containers type.
        """
        self.getParentProject(containerID)
        sql = """SELECT DISTINCT c.CodeContainer, c.ContainerName FROM Containers c, DataValuesR r,
            MetaData m
            WHERE r.DataValue = %d
            AND r.LinkContainer=c.CodeContainer
            AND r.Status = 'Actual'
            AND c.ContainerType='%s'
            AND c.Status = 'Actual'
	    AND r.LinkMetaData=m.CodeData AND m.KeyWord='%s';

        """ % (containerID, container_type, attr_name)
        n = self.c.execute(sql)
        res = self.c.fetchall()
        ans = []
        for i in range(n):
            ans.append(res[i][0])
        return ans


    def getReferencingObjectsWithNames(self, containerID, type):
        """Returns names and id of containers referencing to containerID.
        Output is limited to types of containers type.
        Return: list of tuples:
          [(ID, Name), ...]
          Normally, the output list has only one element (however, the list
          may be empty).
        """
        self.getParentProject(containerID) # this checks access permissions
##        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        # !!!efr - we should select actual containers only
        sql = """SELECT CodeContainer, ContainerName FROM Containers, DataValuesR
            WHERE DataValuesR.DataValue=%d
            AND DataValuesR.LinkContainer=CodeContainer
            AND DataValuesR.Status = 'Actual'
            AND ContainerType='%s'
            AND Containers.Status = 'Actual'
            """ % (containerID, type)
        n = self.c.execute(sql)
        res = self.c.fetchall()
        ans = []
        for i in range(n):
            ans.append(res[i])
        return ans


    def getReferencingObjectsWithNamesAndFixedParent(self, containerID, type, pid):
        """Returns names, id and parent of containers referencing to containerID.
        Output is limited to types of containers type and contaners having pid as parent.
        Return: list of tuples:
          [(ID, Name), ...]
          Normally, the output list has only one element (however, the list
          may be empty).
        """
        self.getParentProject(containerID) # this checks access permissions
##        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        # !!!efr - we should select actual containers only
        sql = """SELECT CodeContainer, ContainerName, LinkUp FROM Containers, DataValuesR
            WHERE DataValuesR.DataValue=%d
            AND DataValuesR.LinkContainer=CodeContainer
            AND DataValuesR.Status = 'Actual'
            AND ContainerType='%s'
            AND Containers.Status = 'Actual'
            AND LinkUp=%d
            """ % (containerID, type, pid)
        n = self.c.execute(sql)
        res = self.c.fetchall()
        ans = []
        for i in range(n):
            ans.append(res[i])
        return ans

    def getReferencedContainersNames(self, prid, master_type, master_name):
        """Returns list of names of parent containers containing containers referenced by
        container of type master_type having a name master_name.
        Input:
          prid - project id
          master_type - type of master container
          master_name - name of master container
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        sql = """
            SELECT c2.ContainerName 
            FROM Containers as c2, Containers as c1, DataValuesR as r, Containers as c3 
            WHERE 
            c3.TopParent=%d 
            AND r.LinkContainer = c3.CodeContainer 
            AND r.DataValue = c1.CodeContainer 
            AND c1.LinkUp = c2.CodeContainer 
            AND c3.ContainerName = "%s" 
            AND c1.Status = "Actual" AND c2.Status = "Actual" AND c3.Status = "Actual" AND r.Status = "Actual" 
            AND c3.ContainerType = "%s" 
            ORDER BY r.ValueIndex
            """ % (prid, master_name, master_type)
        n = self.c.execute(sql)
        res = self.c.fetchall()
        ans = []
        for i in range(n):
            ans.append(res[i][0])
        return ans

    def getMasterAndReferencedContainersNames(self, prid, master_type):
        """Returns list of pairs (master_name, parent_of_referenced_name), where
        parent_of_referenced_name are names of parent containers containing
        containers referenced by container of type master_type having a name master_name
        Input:
          prid - project id
          master_type - type of master container
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        sql = """
            SELECT c3.ContainerName, c2.ContainerName 
            FROM Containers as c2, Containers as c1, DataValuesR as r, Containers as c3 
            WHERE 
            c3.TopParent=%d 
            AND r.LinkContainer = c3.CodeContainer 
            AND r.DataValue = c1.CodeContainer 
            AND c1.LinkUp = c2.CodeContainer 
            AND c1.Status = "Actual" AND c2.Status = "Actual" AND c3.Status = "Actual" AND r.Status = "Actual" 
            AND c3.ContainerType = "%s" 
            ORDER BY r.ValueIndex
            """ % (prid, master_type)
        n = self.c.execute(sql)
        res = self.c.fetchall()
        return res

    def getDistinctReferencedContainersNames(self, prid, cont_type, cont_name, attr_name):
        """Returns list of container names that are referenced by containers with type cont_type
        and name equal to cont_name. Reference comes through X-value attribute named attr_name.
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        sql = """
             SELECT DISTINCT h.ContainerName
             FROM Containers c, Containers h, DataValuesX x, MetaData m 
             WHERE
             c.TopParent=%d AND c.Status='Actual' 
             AND x.Status='Actual' AND x.LinkContainer = c.CodeContainer 
             AND c.ContainerType='%s' 
             AND x.DataValueR=h.CodeContainer 
             AND c.ContainerName='%s' 
             AND x.LinkMetaData=m.CodeData AND m.KeyWord='%s' 
             ORDER BY  h.ContainerName
              """ % (prid, cont_type, cont_name, attr_name)
        n = self.c.execute(sql.encode('utf8'))
        res = self.c.fetchall()
        res1 = [l[0] for l in res]
        return res1
        
    def getDistinctReferencedNamesByObjectsWithParentsInList(self, prid, cont_type, parent_names, cont_name, attr_name):
        """Returns list of container names that are referenced by containers with type cont_type
        and name equal to cont_name. Reference comes through X-value attribute named attr_name.
        """
        assert type(parent_names) == list, 'Invalid type of argument in getDistinctReferencedNamesByObjectsWithParentsInList'
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        parent_names_sql = "', '".join(parent_names)
        sql = """
             SELECT DISTINCT h.ContainerName
             FROM Containers c, Containers p, Containers h, DataValuesX x, MetaData m 
             WHERE
             p.TopParent=%d AND p.Status='Actual' 
             AND  p.ContainerName in ('%s')
             AND c.LinkUp = p.CodeContainer AND c.Status='Actual'
             AND x.Status='Actual' AND x.LinkContainer = c.CodeContainer 
             AND c.ContainerType='%s' 
             AND c.ContainerName='%s' 
             AND x.DataValueR=h.CodeContainer 
             AND x.LinkMetaData=m.CodeData AND m.KeyWord='%s' 
             ORDER BY  h.ContainerName
              """ % (prid, parent_names_sql, cont_type, cont_name, attr_name)
        n = self.c.execute(sql.encode('utf8'))
        res = self.c.fetchall()
        res1 = [l[0] for l in res]
        return res1

    def getMinMaxForParameter(self, prid,  cont_type, attr_name, cont_name = None):
        """Return result of aggregate function applied to values of some attribute
        of containers cont_type having the name cont_name (may be ommited).
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        sql_templ = """
        SELECT MIN(d.DataValue), MAX(d.DataValue) 
        FROM Containers  c, MetaData m, DataValuesD d 
        WHERE c.ContainerType='%s' 
        AND c.TopParent=%d AND c.Status = 'Actual' 
        AND m.ContainerType='%s' AND m.KeyWord='%s' 
        AND d.LinkMetaData = m.CodeData AND d.LinkContainer = c.CodeContainer 
        AND d.Status = 'Actual' 
        AND d.DataValue  < 3.401e+38
        """ 
        if cont_name:
            sql_templ += " AND c.ContainerName = '%s'" % cont_name
        sql = sql_templ  % (cont_type, prid, cont_type, attr_name)
        n = self.c.execute(sql.encode('utf8'))
        res = self.c.fetchall()
        if n:
            res1 = list(res[0])
            if res1[0] is None:
                res1[0] = -MAXFLOAT
            if res1[1] is None:
               res1[1] =  MAXFLOAT
        else:
            res1 = [-MAXFLOAT, MAXFLOAT]  
        return res1

    def getMinMaxForParameterInContainer(self, prid, id,  cont_type, attr_name, cont_name = None):
        """Return result of aggregate function applied to values of some attribute
        of containers cont_type having the name cont_name (may be ommited), defined in a container
        designated by id.
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        sql_templ = """
        SELECT MIN(d.DataValue), MAX(d.DataValue) 
        FROM Containers  c, MetaData m, DataValuesD d 
        WHERE c.ContainerType='%s' 
        AND c.LinkUp=%d AND c.Status = 'Actual' 
        AND m.ContainerType='%s' AND m.KeyWord='%s' 
        AND d.LinkMetaData = m.CodeData AND d.LinkContainer = c.CodeContainer 
        AND d.Status = 'Actual' 
        AND d.DataValue  < 3.401e+38
        """ 
        if cont_name:
            sql_templ += " AND c.ContainerName = '%s'" % cont_name
        sql = sql_templ  % (cont_type, id, cont_type, attr_name)
        n = self.c.execute(sql.encode('utf8'))
        res = self.c.fetchall()
        if n:
            res1 = list(res[0])
            if res1[0] is None:
                res1[0] = -MAXFLOAT
            if res1[1] is None:
               res1[1] =  MAXFLOAT
        else:
            res1 = [-MAXFLOAT, MAXFLOAT]  
        return res1
        

    def markSingleAttributeDeleted(self, containerID, a_name):
        """Mark container attribute as deleted.
        Input:
            containerID - ID of container attribute belongs to
            a_name - name of attribute
        Output:
            >0 if success
        Exceptions:
            - may raise DBException if there is no such attribute in
                container type metadata
            - conversion exceptions may be raised"""
        self.auth.assertNotProtected(containerID)
        (c_type, pr_id) = self.getContainerType(containerID)
        # extract all metadata about container type
        m_d = self.MetaData[c_type]
        a_desc = m_d[a_name]
        # looking at the dimension:
        if a_desc[2] != 0 and not (a_desc[2] is None):
            # this is an array attribute
            raise 'markSingleAttributeDeleted not implemented for array attribute'
        perm_flags = self.auth.getPermissions(pr_id)
        self.auth.checkPermissions(pr_id, Authorities.DELETE_ATTR, perm_flags)
        self.c.execute('LOCK TABLES DataValues%s WRITE, ChangeLog WRITE' % a_desc[0])
        try:
            #a_desc[3] - line number in MetaData
            # getting ID of attribute line
            rc = self.c.execute("""SELECT CodeValue from DataValues%s
                        WHERE LinkContainer = %d
                        AND LinkMetaData = %d
                        AND Status = "Actual" """ % (a_desc[0], containerID, a_desc[3]))
            if rc:
                v_id = self.c.fetchall()[0][0]
            else:
                return 0
            # updating attributes values
            rc = self.c.execute("""UPDATE DataValues%s
                        SET Status = "Deleted"
                        WHERE LinkContainer = %d
                        AND LinkMetaData = %d
                        AND Status = "Actual" """ % (a_desc[0], containerID, a_desc[3]))
            self.auth.addLog('DataValues' + a_desc[0], v_id, 'Delete')
            return rc
        finally:
            self.c.execute('UNLOCK TABLES')

    def markContainerDeleted(self, containerID):
        """Mark container and its attributes as deleted"""
        prid = self.getParentProject(containerID)
        self.auth.checkPermissions(prid, Authorities.DELETE_CONT)
        # Check for protection
        self.auth.assertNotProtected(containerID)
        self.c.execute("""UPDATE Containers
                SET Status = "Deleted"
                WHERE CodeContainer = %d """ % containerID)
        self.auth.addLog('Containers', containerID, 'Delete')
        # now mark all attributes of container as deleted
        self.c.execute("""LOCK TABLES DataValuesC WRITE,
                DataValuesI WRITE,
                DataValuesF WRITE,
                DataValuesD WRITE,
                DataValuesR WRITE,
                DataValuesP WRITE,
                DataValuesT WRITE,
                DataValuesX WRITE, ChangeLog WRITE
                """)
        try:
            tables = ['DataValuesC', 'DataValuesI', 'DataValuesF', 'DataValuesD', 'DataValuesR', 'DataValuesP', 'DataValuesT', 'DataValuesX']
            indices = {}
            for table in tables:
                n = self.c.execute("""SELECT CodeValue FROM %s
                        WHERE  LinkContainer = %d AND Status = 'Actual' """ % (table, containerID))
                idx = self.c.fetchall()
                indices[table] = idx
                n = self.c.execute("""UPDATE %s
                        SET Status = "Deleted"
                        WHERE LinkContainer = %d AND Status = 'Actual' """ % (table, containerID))
            # find and mark as deleted all references to the object
            n = self.c.execute("""UPDATE DataValuesR
                    SET Status = 'Deleted'
                    WHERE DataValue = %d
                    AND Status = 'Actual'
                    """ % containerID)
            n = self.c.execute("""UPDATE DataValuesX
                    SET Status = 'Deleted'
                    WHERE DataValueR = %d
                    AND Status = 'Actual'
                    """ % containerID)
            for table in tables:
                for ndx in indices[table]:
                    self.auth.addLog( table, ndx[0], 'Delete')
        finally:
            self.c.execute("""UNLOCK TABLES""")

    def markContainerDeleted_NEW(self, containerID, force=False):
        """Mark container and its attributes as deleted"""
        prid = self.getParentProject(containerID)
        self.auth.checkPermissions(prid, Authorities.DELETE_CONT)
        # Check for protection
        if not force:
            self.auth.assertNotProtected(containerID)

        # now mark all attributes of container as deleted
        self.c.execute(("update Containers c \n"
                        "left join DataValuesC dc on c.CodeContainer = dc.LinkContainer \n"
                        "left join DataValuesP dp on c.CodeContainer = dp.LinkContainer\n"
                        "left join DataValuesI di on c.CodeContainer = di.LinkContainer\n"
                        "left join DataValuesF df on c.CodeContainer = df.LinkContainer\n"
                        "left join DataValuesD dd on c.CodeContainer = dd.LinkContainer\n"
                        "left join DataValuesR dr on c.CodeContainer = dr.LinkContainer\n"
                        "left join DataValuesX dx on c.CodeContainer = dx.LinkContainer\n"
                        "left join DataValuesT dt on c.CodeContainer = dt.LinkContainer\n"
                        "set c.Status = 'Deleted', dc.Status = 'Deleted', dp.Status = 'Deleted',\n"
                        "di.Status = 'Deleted', df.Status = 'Deleted', dd.Status = 'Deleted',\n"
                        "dr.Status = 'Deleted', dx.Status = 'Deleted', dt.Status = 'Deleted'\n"
                        "where \n"
                        "c.CodeContainer = %d\n"
                        "        ") % (containerID,))
        # find and mark as deleted all references to the object
        n = self.c.execute("""UPDATE DataValuesR
                    SET Status = 'Deleted'
                    WHERE DataValue = %d
                    AND Status = 'Actual'
                    """ % containerID)
        n = self.c.execute("""UPDATE DataValuesX
                    SET Status = 'Deleted'
                    WHERE DataValueR = %d
                    AND Status = 'Actual'
                    """ % containerID)
        self.auth.addLog('Containers', containerID, 'Delete')

    def markContainersTreeDeleted(self, containerID, force=False):
        """Mark the whole tree of containers as deleted"""
        ids = self.getSubContainersList(containerID)
        for c_id in ids:
            self.markContainersTreeDeleted(c_id[0], force)
        self.markContainerDeleted_NEW(containerID, force)

    def markContainersTreeDeleted_NEW(self, containerID, force=False):
        prid = self.getParentProject(containerID)
        self.auth.checkPermissions(prid, Authorities.DELETE_CONT)
        # Check for protection
        if not force:
            self.auth.assertNotProtected(containerID)
        MAX_LEVEL = 5  # @TODO: We should eliminate the magic constant!
        for i in range(MAX_LEVEL-1, 0-1, -1):
            self.markChildrenOfLevelNDeleted(i, containerID)
        self.auth.addLog('Containers', containerID, 'DeleteTree')
        self.markLooseReferencesDeleted(prid)

    def markChildrenOfLevelNDeleted(self, n, containerID):
        "Generate and execute SQL to delete childen of a given container at tle level N"
        # Generating sql statement
        sql_update = ["update Containers c0"]
        for i in range(n):
            sql_update.append("Containers c%d on c%d.LinkUp = c%d.CodeContainer" % (i+1, i+1, i))
        sql_set = " set c%d.Status='Deleted' " % n
        sql_where = ' where ' + ' and '.join(["c%d.Status = 'Actual' " % i for i in range(n)] + ["c0.CodeContainer = %d " % containerID])
        sql = " join ".join(sql_update) + sql_set + sql_where
        ## print 'DEBUG: ', sql
        self.c.execute(sql)
        self.auth.addLog('Containers', containerID, 'Delete')

    def markRefsToObjDeleted(self, pr_id, fid):
        """Mark references to container with fid as deleted
        Returns None
        Exceptions:
          May raise authority exception"""
        # check permissions
        self.auth.checkPermissions(pr_id, Authorities.UPDT_ATTR)
##        sql = """UPDATE DataValuesR, Containers
##        SET DataValuesR.Status = "Deleted"
##        WHERE DataValue = %d
##        AND Containers.Status = "Actual"
##        AND Containers.TopParent = %d
##        AND DataValuesR.Status = "Actual"
##        AND DataValuesR.LinkContainer =  Containers.CodeContainer""" % (fid, pr_id)
        sql = """UPDATE DataValuesR
        SET DataValuesR.Status = 'Deleted'
        WHERE DataValue = %d
        AND DataValuesR.Status = 'Actual'  """ % fid
        n = self.c.execute(sql)
        sql = """UPDATE DataValuesX
        SET Status = 'Deleted'
        WHERE DataValueR = %d
        AND Status = 'Actual'  """ % fid
        n = self.c.execute(sql)

    def markLooseReferencesDeleted(self, pr_id):
        """
        Mark all actual references (from tables DataValuesR and DataValuesX) pointing at
        deleted containers as Deleted.
        :param pr_id:
        :return: None
        """
        sqlR = "update Containers c left join DataValuesR r on r.DataValue = c.CodeContainer set r.Status='Deleted' where c.TopParent = %d and c.Status='Deleted' and r.Status='Actual'"
        sqlX = "update Containers c left join DataValuesX r on r.DataValueR = c.CodeContainer set r.Status='Deleted' where c.TopParent = %d and c.Status='Deleted' and r.Status='Actual'"
        n = self.c.execute(sqlR % (pr_id,))
        n = self.c.execute(sqlX % (pr_id,))

    def undeleteContainer(self, containerID):
        'Undo the operation of deletion of container. Returns new (unique) name that restored container gets.'
        # Note: we need to lock the UserPermissions table as p because it is used in the new version of getPermissions that
        # allows any container id as argument (previous version reqiured project id).
        self.c.execute("""LOCK TABLES Containers WRITE, Containers as c WRITE, Containers as pc WRITE,
            UserPermissions as p WRITE,
            DataValuesC WRITE,
            DataValuesI WRITE,
            DataValuesF WRITE,
            DataValuesD WRITE,
            DataValuesR WRITE,
            DataValuesP WRITE,
            DataValuesT WRITE,
            DataValuesX WRITE,
            MetaData as m WRITE,
            UserPermissions WRITE, ChangeLog WRITE""")
        try:
           new_name = self.undeleteContainerNoLock(containerID)
           return new_name
        finally:
            self.c.execute("""UNLOCK TABLES""")
            self.undeleteReferencesToContainer(containerID, 'act1') # @TODO: not very safe, but will do so far

    def undeleteReferencesToContainer(self, containerID, referencingType):
        "Make sure references to an actual container from another actual container of type referencingType are actual as well"
        projId = self.getParentProject(containerID)
        sql1 = """select  max(r.CodeValue)
        from Containers c1 join DataValuesR r on r.LinkContainer = CodeContainer
        join Containers c2 on r.DataValue = c2.CodeContainer
        where c1.Status='Actual' and
        c2.Status = 'Actual' and
        c1.TopParent = %d and
        c1.ContainerType='%s' and
        c2.CodeContainer = %d group by c1.CodeContainer""" % (projId, referencingType, containerID)
        # print 'DEBUG: undeleteReferencesToContainer', sql1
        self.c.execute(sql1)
        refs = self.c.fetchall()
        for i in refs:
            sql2 = "update DataValuesR set Status = 'Actual' where CodeValue = %d" % i[0]
            # print 'DEBUG: undeleteReferencesToContainer', sql2
            self.c.execute(sql2)



    def undeleteContainerNoLock(self, containerID):
        """Undo the operation of deletion of container. Returns new (unique) name that restored container gets.
        May return None if undeleting a container inside deleted tree of cantainers was attempted."""
        new_name = None
        ## raise RuntimeWarning('Not implemented yet!')
        ## we should first check that the container is really deleted
        n = self.c.execute("SELECT Status FROM Containers WHERE CodeContainer = %d" % containerID)
        if (n == 0) or (self.c.fetchall()[0][0] == 'Actual'):
            return
        try:
            # make sure parent is not deleted
            self.c.execute("""SELECT pc.Status = 'Actual' FROM Containers c, Containers pc WHERE c.CodeContainer = %d and c.LinkUp = pc.CodeContainer""" % containerID)
            # assert self.c.fetchall()[0][0], 'Cannot undelete container %d having deleted parent' % containerID
            if not self.c.fetchall()[0][0]:
                raise DBException('Cannot undelete container %d having deleted parent' % containerID)

            # make sure container has unique name
            c_type, c_name = self.getContainerName(containerID, False)
            pc = self.getParentContainer(containerID)
            c_names = [i[2] for i in self.getSubContainersList(pc)]
            r_names = []  # here we will put all the names of referenced horizons
            horlc_ids = []  # ids of horl or horc objects to be renamed later
            fltlc_ids = []  # ids of fltl or fltc objects to be renamed later
            # get all attributes and undelete them
            m_d = self.MetaData[c_type]
            for a_name in list(m_d.keys()):
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
                table_name = 'DataValues' + form
                sql = "select max(CodeValue), ValueIndex from %s, MetaData m  WHERE LinkContainer = %d AND LinkMetaData = m.CodeData AND m.KeyWord = '%s'  group by LinkMetaData, ValueIndex" % (table_name, containerID, a_name)
                n = self.c.execute(sql)
                codes_2undel = [i[0] for i in self.c.fetchall()]
                if table_name.lower() == "datavaluesr":
                    # update references
                    for attr_id in codes_2undel:
                        # fetch the referenced container ID
                        self.c.execute("SELECT DataValue FROM DataValuesR WHERE CodeValue = %d" % attr_id)
                        undel_cont_id = self.c.fetchall()[0][0]
                        try:
                            undel_cont_newname = self.undeleteContainerNoLock(undel_cont_id)
                        except DBException as ex:
                            continue  # Just ignore an attempt to restore container that has a deleted parent
                        self.c.execute(""" update DataValuesR set Status = 'Actual' where CodeValue = %d """ % attr_id)
                        ## @TODO: faults and horizons require special care: name of horizon is duplicated in objects being pointed by attributes
                        if (c_type == 'hor') and (a_name.lower() in ['refs2horl', 'refs2horc']):
                            horlc_ids.append(undel_cont_id)
                            pc = self.getParentContainer(undel_cont_id) # Here parent container may be deleted
                            r_tmp = self.getSubContainersListByType(pc, a_name.lower().split('2')[1])  # 2nd argument should be 'horc' or 'horl'
                            c_names += [i[1].split('#', 1)[1] for i in r_tmp]
                        elif (c_type == 'flt') and (a_name.lower() in ['refs2fltl', 'refs2fltc']):
                            fltlc_ids.append(undel_cont_id)
                            pc = self.getParentContainer(undel_cont_id) 
                            r_tmp = self.getSubContainersListByType(pc, a_name.lower().split('2')[1])  # 2nd argument should be 'fltc' or 'fltl'
                            c_names += [i[1].split('#', 1)[1] for i in r_tmp]

                elif table_name.lower() == "datavaluesx":
                    # update cross-references
                    for attr_id in codes_2undel:
                        # fetch the referenced container ID
                        self.c.execute("SELECT DataValueR FROM DataValuesX WHERE CodeValue = %d" % attr_id)
                        undel_cont_id = self.c.fetchall()[0][0]
                        undel_cont_newname = self.undeleteContainerNoLock(undel_cont_id)
                        self.c.execute(""" update DataValuesX set Status = 'Actual' where CodeValue = %d """ % attr_id)
                else:
                    for attr_id in codes_2undel:
                        self.c.execute(""" UPDATE %s SET Status = 'Actual' WHERE CodeValue=%d """ % (table_name, attr_id))
            # compute the new name taking into account names of horl and horc objects referenced by horizons
            #print 'DEBUG: c_names', c_names
            new_name =  makeUniqueNameNew(c_name, c_names, ' ')
            # Rename the referenced horl, horc, fltl, fltc objects in accordance with new_name!
            for r_id in horlc_ids:
                self.c.execute((""" UPDATE Containers SET ContainerName = '%s', Status = 'Actual' WHERE CodeContainer = %d""" % ('Horizon#' + new_name, r_id)).encode('utf8'))
                self.auth.addLog('Containers', r_id, 'Rename')
                self.auth.addLog('Containers', r_id, 'Restore')
            for r_id in fltlc_ids:
                self.c.execute((""" UPDATE Containers SET ContainerName = '%s', Status = 'Actual' WHERE CodeContainer = %d""" % ('Fault#' + new_name, r_id)).encode('utf8'))
                self.auth.addLog('Containers', r_id, 'Rename')
                self.auth.addLog('Containers', r_id, 'Restore')
            # rename to new name and undelete
            self.c.execute((""" UPDATE Containers SET ContainerName = '%s', Status = 'Actual' WHERE CodeContainer = %d""" % (new_name, containerID)).encode('utf8'))
            if c_name != new_name:
                self.auth.addLog('Containers', containerID, 'Rename')
        except Exception as ex:
            raise ex
        self.auth.addLog('Containers', containerID, 'Restore')
        return new_name


    def getNumberContainersInProject(self, pr_id):
        """Count and return number of containers included in project"""
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        sql = """SELECT COUNT(*) FROM Containers
	        WHERE TopParent = %d AND Status = "Actual" """ % (pr_id)
        n = self.c.execute(sql)
        return self.c.fetchall()[0][0]

    def getAttributeChangeHistoryByOid(self, oid, attr_name):
        """
        Return list of tuples describing change history of an attribute.
        :param oid: Object ID
        :param attr_name: Name of attribute
        :return: [(attr_id:int, is_actual: int, data_value: str, creation_timestamp: date_time, username: str), ...]
        """
        self.c.execute("select ContainerType from Containers where CodeContainer=%d" % oid)
        c_type = self.c.fetchall()[0][0]
        m_d = self.MetaData[c_type]
        (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[attr_name]
        assert dim == 0, 'Arrays are not yet supported in getAttributeChangeHistoryByOid'
        if form != 'C':
            raise DBException('getAttributeChangeHistoryByOid supports only char attributes')
        # sql = "select CodeValue, Status='Actual', DataValue from DataValuesC where LinkContainer=%d and LinkMetaData=%d order by CodeValue desc" % (oid, md_id)
        sql = """select a.CodeValue, a.Status='Actual', a.DataValue,  c.Modified, u.UserName from DataValuesC a, ChangeLog c, Users u where
                a.LinkContainer=%d and a.LinkMetaData=%d
                and c.TableType='DataValuesC' and a.CodeValue = c.Link and
                c.Operation='Create'
                and c.UserID = u.UserID
                """ % (oid, md_id)
        self.c.execute(sql)
        return self.c.fetchall()

    def getArrayAttributeChangeHistoryByOid(self, oid, attr_name):
        """
        Same as getAttributeChangeHistoryByOid, but acting on array attributes
        :param oid:
        :param attr_name:
        :return: [(attr_id:int, index: int, is_actual: int, data_value: str, creation_timestamp: date_time, username: str), ...]
        """
        self.c.execute("select ContainerType from Containers where CodeContainer=%d" % oid)
        c_type = self.c.fetchall()[0][0]
        m_d = self.MetaData[c_type]
        (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[attr_name]
        assert dim == 1, 'Scalars are supported by getAttributeChangeHistoryByOid'
        if form != 'X':
            raise DBException('getAttributeChangeHistoryByOid supports only pairs (double, ref)')
        # NOTE: Here we are implicitly using the fact that only the last array element is mentioned in the ChangeLog
        # table with the 'Create' operation
        sql = """select a.CodeValue, a.ValueIndex, a.Status='Actual', a.DataValueR,  c.Modified, u.UserName from DataValuesX a, ChangeLog c, Users u where
                a.LinkContainer=%d and a.LinkMetaData=%d
                and c.TableType='DataValuesX' and a.CodeValue = c.Link and
                c.Operation='Create'
                and c.UserID = u.UserID
                """ % (oid, md_id)
        self.c.execute(sql)
        return self.c.fetchall()

    def getCAttributeById(self, aid):
        sql = """select a.DataValue from  DataValuesC a where a.CodeValue=%d""" % aid
        self.c.execute(sql)
        return self.c.fetchall()[0][0]

    def getVersion(self):
        """ return [Version, Modification, DateOfVersion] for used DB """
        sql = """SELECT Version, Modification, DateVersion from ParamTable"""
        n = self.c.execute(sql)
        return self.c.fetchall()[0]
        
###########################################################
#  Misc. functions - utilities
###########################################################
    def getContainerNamesByTypeCountParentsAndCAttrMissingAsNone(self, prid, c_type, a_name):
        """Get distinct triples cosisting of names of all containers having type c_type, 
        number of parents and value of character attribute.
        Undefined values of attributes returned as None.
        Returns:
        [(Parents_No, Name, Attr_value), ... ]
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        m_d = self.MetaData[c_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (c_type, a_name))
        if dim:
            raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone does not support array attributes (%s is aray for %s)' %
                              (a_name, c_type))
        if form != 'C':
            raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone supports only char attributes')
        sql = """
               SELECT COUNT(c1.LinkUp), c1.ContainerName, v.DataValue FROM 
               Containers AS c1 LEFT JOIN  DataValuesC AS v 
               ON v.LinkContainer = c1.CodeContainer AND v.Status = "Actual" AND v.LinkMetaData=%d 
               WHERE  c1.TopParent = %d 
               AND c1.ContainerType = "%s" 
               AND c1.Status = "Actual" 
               GROUP BY c1.ContainerName, v.DataValue;        
          """ % (md_id, prid, c_type)
        n = self.c.execute(sql.encode('utf8'))
        return self.c.fetchall()

    def getContainerNamesByTypeCountParentsAndCAttrMissingAsNoneMt(self, prid, c_type, a_name, a_name_mod):
        """Get distinct triples cosisting of names of all containers having type c_type,
        number of parents and value of character attribute.
        Undefined values of attributes returned as None.
        Modification time of a_name_mod is returned as the component #3 of output tuples.
        Returns:
        [(Parents_No, Name, Attr_value, Mod_time), ... ]
        """
        def get_attr_id(m_d, a_name):
            try:
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
            except KeyError:
                raise DBException('Container type %s has no attribute %s' % (c_type, a_name))
            if dim:
                raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone does not support array attributes (%s is aray for %s)' %
                                  (a_name, c_type))
            if form != 'C':
                raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone supports only char attributes')
            return md_id

        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        m_d = self.MetaData[c_type]
        md_id = get_attr_id(m_d, a_name)
        md_mod_id = get_attr_id(m_d, a_name_mod)
        sql = """
               SELECT COUNT(c1.LinkUp), c1.ContainerName, v.DataValue, MAX(l.Modified) FROM
               Containers AS c1 LEFT JOIN  DataValuesC AS v
               ON v.LinkContainer = c1.CodeContainer AND v.Status =
               "Actual" AND v.LinkMetaData=%d
	       LEFT JOIN DataValuesC p ON p.LinkContainer =
               c1.CodeContainer AND p.Status='Actual' and
               p.LinkMetaData = %d
	       LEFT JOIN ChangeLog l ON p.CodeValue = l.Link AND
               l.Operation = 'Create' AND l.TableType = 'DataValuesC'
               WHERE  c1.TopParent = %d
               AND c1.ContainerType = '%s'
               AND c1.Status = 'Actual'
	       GROUP BY c1.ContainerName, v.DataValue
          """ % (md_id, md_mod_id, prid, c_type)
        n = self.c.execute(sql.encode('utf8'))
        return self.c.fetchall()

    def getContainerNamesByTypeCountParentsAndCAttrMissingAsNoneMtRn(self, prid, c_type, a_name, mod_name, mc_names):
        """Get distinct triples cosisting of names of all containers having type c_type,
        number of parents and value of character attribute.
        Undefined values of attributes returned as None.
        Names of intermediate containers can be restricted to belong to a given list.
        Modification time of m_name is returned as well.
        Returns:
        [(Parents_No, Name, Attr_value, Mod_time), ... ]
        """
        def get_attr_id(m_d, a_name):
            try:
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
            except KeyError:
                raise DBException('Container type %s has no attribute %s' % (c_type, a_name))
            if dim:
                raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone does not support array attributes (%s is aray for %s)' %
                                  (a_name, c_type))
            if form != 'C':
                raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone supports only char attributes')
            return md_id

        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        m_d = self.MetaData[c_type]
        md_id = get_attr_id(m_d, a_name)
        md_mod_id = get_attr_id(m_d, mod_name)
        if mc_names:
            sql_restr = " AND c2.containerName in (" + ",".join(["'"+s+"'" for s in mc_names]) + ')'
        else:
            sql_restr = ""
        sql = """
               SELECT COUNT(c1.LinkUp), c1.ContainerName, v.DataValue, MAX(l.Modified) FROM
               Containers AS c2, Containers AS c1 LEFT JOIN  DataValuesC AS v
               ON v.LinkContainer = c1.CodeContainer AND v.Status =
               "Actual" AND v.LinkMetaData=%d
	       LEFT JOIN DataValuesC p ON p.LinkContainer =
               c1.CodeContainer AND p.Status='Actual' and
               p.LinkMetaData = %d
	       LEFT JOIN ChangeLog l ON p.CodeValue = l.Link AND
               l.Operation = 'Create' AND l.TableType = 'DataValuesC'
               WHERE  c2.TopParent = %d
	       AND c1.LinkUp = c2.CodeContainer
               AND c1.ContainerType = '%s'
               AND c1.Status = 'Actual'
               AND c2.Status = 'Actual'
               %s
               GROUP BY c1.ContainerName, v.DataValue
          """ % (md_id, md_mod_id, prid, c_type, sql_restr)
        n = self.c.execute(sql.encode('utf8'))
        return self.c.fetchall()

    def getContainerNamesByTypeCountParentsAndCAttrMissingAsNoneRestrictNames(self, prid, c_type, a_name, mc_names):
        """Get distinct triples cosisting of names of all containers having type c_type, 
        number of parents and value of character attribute.
        Undefined values of attributes returned as None.
        Returns:
        [(Parents_No, Name, Attr_value), ... ]
        """
        self.auth.checkPermissions(prid, Authorities.ACCESS_PROJ)
        m_d = self.MetaData[c_type]
        try:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a_name]
        except KeyError:
            raise DBException('Container type %s has no attribute %s' % (c_type, a_name))
        if dim:
            raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone does not support array attributes (%s is aray for %s)' %
                              (a_name, c_type))
        if form != 'C':
            raise DBException('getContainerNamesByTypeCountParentsAndCAttrMissingAsNone supports only char attributes')
        if mc_names:
            sql_restr = " AND c2.containerName in (" + ",".join(["'"+s+"'" for s in mc_names]) + ')'
        else:
            sql_restr = ""
        sql = """
               SELECT COUNT(c1.LinkUp), c1.ContainerName, v.DataValue FROM 
               Containers AS c2, Containers AS c1 LEFT JOIN  DataValuesC AS v 
               ON v.LinkContainer = c1.CodeContainer AND v.Status = "Actual" AND v.LinkMetaData=%d 
               WHERE  c2.TopParent = %d 
	       AND c1.LinkUp = c2.CodeContainer
               AND c1.ContainerType = "%s" 
               AND c1.Status = "Actual" 
               AND c2.Status = "Actual" 
               %s
               GROUP BY c1.ContainerName, v.DataValue;        
          """ % (md_id, prid, c_type, sql_restr)
        n = self.c.execute(sql.encode('utf8'))
        return self.c.fetchall()

    def renameConnectedContainers(self, pr_id, type1, type2, prefix, old_name, new_name):
        """Rename containers when names have correspondance.
        E.g. containers of type1 are named <name>, and corresponding
        containers having type type2 are named <prefix><name>.
        Input:
          - pr_id - project id
          - type1 - type of containers without prefix
          - type2 - type of containers with prefix. May be set of types
          - prefix - prefix in the name
          - old_name - old name
          - new_name new name
        Output:
          None
        Exceptions:
          Exception may be raised when renaming leads to non-unique
          names (currently names must be globally unique)
        """
        # check permissions
        self.auth.checkPermissions(pr_id, Authorities.UPDT_ATTR)

        # Strip and check the validity of name
        new_name = new_name.strip()
        if not new_name:
            raise DBException('Renaming container to empty name attempted')
 
        if type(type2) == list:
            sql_where_short = ' AND ContainerType IN ('
            sql_where = ' AND c.ContainerType IN ('
            suppl_q2 = ','.join(['"' + s + '"' for s in type2])
            sql_where = sql_where + suppl_q2 + ') '
            sql_where_short = sql_where_short + suppl_q2 + ') '
        else:
            sql_where = ' AND c.ContainerType="%s" ' % type2
            sql_where_short =  ' AND ContainerType="%s" ' % type2
        old_prefixed_name = prefix + old_name
        new_prefixed_name = prefix + new_name
        self.c.execute("""LOCK TABLES Containers WRITE, Containers as c WRITE, Containers as cp WRITE, ChangeLog WRITE""")
        try:
            sql = """SELECT cp.CodeContainer, c.CodeContainer
                                  FROM Containers AS c, Containers as cp
                                  WHERE c.TopParent=%d
                                  AND cp.LinkUp = c.LinkUp
                                  AND c.Status = 'Actual'
                                  AND cp.Status='Actual'
                                  AND cp.ContainerName='%s'
                                  AND c.ContainerName = '%s'
                                  AND c.ContainerType = '%s' """ % (pr_id, new_name, old_name, type1)
            n = self.c.execute(sql.encode('utf8'))
            if n:
                raise DBException('Duplicate container name %s' % new_name)
            sql = ("""SELECT cp.CodeContainer, c.CodeContainer
                                  FROM Containers AS c, Containers as cp
                                  WHERE c.TopParent=%d
                                  AND cp.LinkUp = c.LinkUp
                                  AND c.Status = 'Actual'
                                  AND cp.Status='Actual'
                                  AND cp.ContainerName='%s'
                                  AND c.ContainerName = '%s' """ +
                               sql_where )  % (pr_id, new_prefixed_name, old_prefixed_name)
            n = self.c.execute(sql.encode('utf8'))
            if n:
                raise DBException('Duplicate container name %s' % new_prefixed_name)
            sql = """SELECT CodeContainer from Containers
                        WHERE TopParent=%d
                        AND ContainerType="%s"
                        AND Status = 'Actual'
                        AND ContainerName="%s" """ % (pr_id, type1, old_name)
            n = self.c.execute(sql.encode('utf8'))
            renamedIDs = self.c.fetchall()
            # now - actually renaming
            sql = """UPDATE Containers
                        SET ContainerName="%s"
                        WHERE TopParent=%d
                        AND ContainerType="%s"
                        AND Status = 'Actual'
                        AND ContainerName="%s" """ % (new_name, pr_id, type1, old_name)
            n = self.c.execute(sql.encode('utf8'))
            sql_related = ("""SELECT CodeContainer FROM Containers
                        WHERE TopParent=%d """  +
                                sql_where_short +
                        """ AND Status = 'Actual'
                        AND ContainerName="%s" """) % (pr_id, old_prefixed_name)
            self.c.execute(sql_related.encode('utf8'))
            renamedIDs += self.c.fetchall()
            sql = ("""UPDATE Containers
                        SET ContainerName="%s"
                        WHERE TopParent=%d """  +
                                sql_where_short +
                        """ AND Status = 'Actual'
                        AND ContainerName="%s" """) % (new_prefixed_name, pr_id, old_prefixed_name)
            n = self.c.execute(sql.encode('utf8'))
            for nContainer in renamedIDs:
                self.auth.addLog('Containers', nContainer[0], 'Rename')
        finally:
            self.c.execute("""UNLOCK TABLES""")

    def executeQuery(self, pr_id, sql):
        """Enables to execute any SQL query allowed for panusr user.
        
        """
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        # check for prohibited words
        op = sql.split()[0].upper()
        if op != 'SELECT':
            raise DBAuthoritiesException('Denied operation %s in project ID %d' % (op, pr_id))
        n = self.c.execute(sql)
        return self.c.fetchall()

    def getIdsNyNameAndType(self, nm, t):
        """Find ids of objects by name and type.
        """
        sql = """SELECT  CodeContainer FROM Containers WHERE
        ContainerName='%s'
        AND ContainerType = '%s'
        AND Status = 'Actual' """ % (nm, t)
        n = self.c.execute(sql)
        tmp = self.c.fetchall()
        ans = []
        for i in tmp:
            ans.append(i[0])
        return ans

    def countParentContainersBySubcontainerName(self, prid, type_upper, type_lower):
        """Cont continers having type type_upper that contain subcontainers with type type_lower,
        group the result by names of subcontainers.
        """
        sql = """
             select cd.ContainerName, count(cu.ContainerName) from Containers cu, Containers cd 
             where cu.TopParent = %d and cu.ContainerType='%s' and cu.CodeContainer=cd.LinkUp 
             and cd.ContainerType = '%s' and cu.Status='Actual' and cd.Status='Actual'
             group by cd.ContainerName """ % (prid, type_upper, type_lower)
        n = self.c.execute(sql)
        tmp = self.c.fetchall()
        return tmp

    def countParentContainersBySubcontainerNameConstraintByList(self, prid, c_list, type_upper, type_lower):
        """Cont continers having type type_upper that contain subcontainers with type type_lower,
        group the result by names of subcontainers. Additional constraint is that upper continers ids must
        be in list c_list.
        """
        c_list_str = '( ' + ', '.join(map(str, c_list)) + ' )'
        sql = """
             select cd.ContainerName, count(cu.ContainerName) from Containers cu, Containers cd 
             where cu.TopParent = %d and cu.ContainerType='%s' and cu.CodeContainer=cd.LinkUp 
             and cd.ContainerType = '%s' and cu.Status='Actual' and cd.Status='Actual'
             and cu.CodeContainer in %s
             group by cd.ContainerName """ % (prid, type_upper, type_lower, c_list_str)
##        print 'DEBUG: countParentContainersBySubcontainerNameConstraintByList', sql
        n = self.c.execute(sql)
        tmp = self.c.fetchall()
        return tmp

    def countParentContainersBySubcontainerNameAndAttr(self, prid, type_upper, type_lower, attr_name):
        """Cont continers having type type_upper that contain subcontainers with type type_lower,
        group the result by names of subcontainers. Additional constraint is that upper continers ids must
        be in list c_list.
        """
        sql = """
             select cd.ContainerName, t.DataValue, count(cu.ContainerName) from Containers cu,
             Containers cd, DataValuesC t, MetaData m
             where cu.TopParent = %d and cu.ContainerType='%s' and cu.CodeContainer=cd.LinkUp
             and cd.ContainerType = '%s' and cu.Status='Actual' and
             cd.Status='Actual'
	     and m.ContainerType='%s'
	     and m.KeyWord='%s'
	     and t.LinkMetaData = m.CodeData
	     and t.LinkContainer = cd.CodeContainer
	     and t.Status='Actual'
             group by cd.ContainerName, t.DataValue """ % (prid, type_upper, type_lower, type_lower, attr_name)
##        print 'DEBUG: countParentContainersBySubcontainerNameConstraintByList', sql
        n = self.c.execute(sql)
        tmp = self.c.fetchall()
        return tmp

    def countParentContainersBySubcontainerNameAndAttrConstraintByList(self, prid, c_list, type_upper, type_lower, attr_name):
        """Cont continers having type type_upper that contain subcontainers with type type_lower,
        group the result by names of subcontainers. Additional constraint is that upper continers ids must
        be in list c_list.
        """
        c_list_str = '( ' + ', '.join(map(str, c_list)) + ' )'
        sql = """
             select cd.ContainerName, t.DataValue, count(cu.ContainerName) from Containers cu,
             Containers cd, DataValuesC t, MetaData m
             where cu.TopParent = %d and cu.ContainerType='%s' and cu.CodeContainer=cd.LinkUp
             and cd.ContainerType = '%s' and cu.Status='Actual' and
             cd.Status='Actual'
	     and m.ContainerType='%s'
	     and m.KeyWord='%s'
	     and t.LinkMetaData = m.CodeData
	     and t.LinkContainer = cd.CodeContainer
	     and t.Status='Actual'
             and cu.CodeContainer in %s
             group by cd.ContainerName, t.DataValue """ % (prid, type_upper, type_lower, type_lower, attr_name, c_list_str)
##        print 'DEBUG: countParentContainersBySubcontainerNameConstraintByList', sql
        n = self.c.execute(sql)
        tmp = self.c.fetchall()
        return tmp


#######################################################
## New scheme of data access 
## Disclamer: methods in this block are for test purposes only
## and are subject to change without notice.
#######################################################
    # this list will be generated automatically from class diagram:
#    containerTypes = {'Repository': 'root',  
#                      'Project': 'proj', 
#                      'MetaInformation': 'meta',
#                      'GeologicObjects': 'geo1',
#                      'Fault': 'flt',
#                      'Horizon': 'hor',
#                      'FaultLine': 'fln1',
#                      'Map': 'map', 
#                      'Grid2D': 'grd2', 
#                      'Well': 'wel1', 
#                      'LogData': 'weld'
#                      }
    containerTypes = className2classIdString # comes from classes_def, which in its own turn is generated from the diagram of classes.
    def type2classIdString(self, className):
        '''Convert readable class name (as known from class diagram) to short
        id valid for concrete relational imnplementation.
        '''
        return self.containerTypes[className]

    def classIdString2type(self, classId):
        '''Convert classId back to user-friendly class name '''
        for c in self.containerTypes:
            if self.containerTypes[c] == classId:
                return c
        #raise KeyError("No such class id: %s" % classId)
        return classId

    def getSubContainersListNew(self, project_name, parentType, parentName, childType):
        '''Return list of names of containers.
        '''
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        parentType_s = self.type2classIdString(parentType)
        childType_s = self.type2classIdString(childType)
        sql = """
        SELECT c.ContainerName FROM Containers p, Containers c
        WHERE p.TopParent = %d AND c.TopParent = %d AND p.Status = 'Actual' AND c.Status = 'Actual'
        AND c.LinkUp = p.CodeContainer AND p.ContainerType = '%s' AND p.ContainerName = '%s' 
        AND c.ContainerType = '%s'
        """ % (pr_id, pr_id, parentType_s, parentName, childType_s)
        self.c.execute(sql.encode('utf8'))
        ans = [a[0] for a in self.c.fetchall()]
        return ans
        
    def getContainerAttributesNew(self, pr_id, parentType, parentName, childType, childName):
        '''Get all attributes of container described by 4-tuple
        '''
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        parentType_s = self.type2classIdString(parentType)
        childType_s = self.type2classIdString(childType)
        # find container designated by (parentType, parentName, childType, childName)
        sql = """
        SELECT c.CodeContainer FROM Containers p, Containers c
        WHERE p.TopParent = %d AND c.TopParent = %d AND p.Status = 'Actual' AND c.Status = 'Actual'
        AND c.LinkUp = p.CodeContainer AND p.ContainerType = '%s' AND p.ContainerName = '%s' 
        AND c.ContainerType = '%s' AND c.ContainerName = '%s'
        """ % (pr_id, pr_id, parentType_s, parentName, childType_s, childName)
        n = self.c.execute(sql.encode('utf8'))
        if not n:
            raise DBNotFoundException('Container designated by (%s, %s, %s, %s) not found' % (parentType, parentName, childType, childName))
        if n > 1:
            raise DBException('Multiple containers designated by (%s, %s, %s, %s)' % (parentType, parentName, childType, childName))
        c_id = self.c.fetchall()[0][0]
        # get attributes of container and sort out vector attributes
        m_d = self.MetaData[childType_s]
        a_list = []
        for a in m_d:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a]
            if dim != 0:
                continue
            a_list.append(a)
##        ans = self.getSubContainersListWithAttributesMissingAsNone(c_id, childType_s, a_list)
        ans = self.getContainerAttributes(c_id)
        ans['#oid#'] = c_id # special internal attribute
        return ans

    def getSubContainersListWithAttributesMissingAsNoneNew(self, project_name, parentType, parentName, childType):
        '''Get list of subcontainers with attributes
        '''
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        parentType_s = self.type2classIdString(parentType)
        childType_s = self.type2classIdString(childType)
        sql = """
        SELECT DISTINCT p.CodeContainer FROM Containers p, Containers c
        WHERE p.TopParent = %d AND c.TopParent = %d AND p.Status = 'Actual' AND c.Status = 'Actual'
        AND c.LinkUp = p.CodeContainer AND p.ContainerType = '%s' AND p.ContainerName = '%s' 
        AND c.ContainerType = '%s'
        """ % (pr_id, pr_id, parentType_s, parentName, childType_s)
        n = self.c.execute(sql.encode('utf8'))
        if not n:
            return []
        p_id = self.c.fetchall()[0][0]
        # get attributes of container and sort out vector attributes
        m_d = self.MetaData[childType_s]
        a_list = []
        a_types = []
        for a in m_d:
            (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a]
            if dim != 0:
                continue
            a_list.append(a)
            a_types.append(form)
        res = []
        attrs = self.getSubContainersListWithAttributesMissingAsNone(p_id, childType_s, a_list)
        for a in attrs:
            oid = a[0]
            name = a[1]
            cur_attr = {'name': name, '#oid#': oid, '#path#': [(parentType, parentName), (childType, name)]}
            corr = 0
            for i in range(len(a_list)):
                if a_types == 'P':
                    cur_attr[str(a_list[i])] = [a[i + 2 + corr], a[i + 3 + corr], a[i + 4 + corr]]
                    corr += 2
                else:
                    if not (a[i+2] is None):
                        cur_attr[str(a_list[i])] = a[i + 2 + corr]
            res.append(cur_attr)
        return res

    def getSubContainersListByPath(self, project_name, path):
        '''Get attributes of sub-containers designated by path.
        Path is a sequence of pairs (Type, name).
        '''
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        assert len(path) > 0, 'Zero-length path'
        select_part = "SELECT "
        for i in range(len(path) - 1):
            select_part += "p%d.ContainerType, p%d.ContainerName, " % (i, i)
        from_part = " p%d.CodeContainer \nFROM " % (len(path) - 1)
        where_part = "WHERE  \n"
        ind = 0
        for c in path:
            if ind == 0:
                from_part += " Containers p%d" % ind
            else:
                from_part += ", Containers p%d" % ind
            assert len(c) > 0, '(Type, Name) pairs must have Type component.'
            if ind:
                where_part += " AND "
            where_part += " p%d.Status='Actual' AND p%d.ContainerType='%s'  AND p%d.TopParent=%d " % (ind, ind, self.type2classIdString(c[0]), ind, pr_id)
            if len(c) > 1:
                if type(c[1]) == list:
                    if c[1]:  # empty list considered as no constraint!
                        where_part += " AND  p%d.ContainerName IN ('" % ind
                        where_part += "', '".join(c[1])
                        where_part += "') "
                else:
                    where_part += " AND  p%d.ContainerName='%s' " % ( ind, c[1])
            if ind == 0:
                where_part += " AND p%d.LinkUp=%d \n" % (ind, pr_id)
            else:
                where_part += " AND p%d.LinkUp=p%d.CodeContainer \n" % (ind, ind-1)
            ind += 1
        sql = select_part + from_part + '\n' + where_part
        n = self.c.execute(sql.encode('utf8'))
        if not n:
            return []
        res = []
        for c_id in self.c.fetchall():
            ans = self.getContainerAttributes(c_id[-1])
##            print 'DEBUG:', ans
            t, name = self.getContainerName(c_id[-1])
            # replace references with type and name of referenced container
            m_d = self.MetaData[t]
            for a in ans:
                (form, sign_val, dim, md_id, ref_type, link_perms) = m_d[a]
                if form == 'R' and dim == 0:
                    ans[a] = (self.getContainerName(ans[a])[1], 
                              self.getContainerName(self.getParentContainer(ans[a]))[1],
                              ans[a])
                elif form == 'R' and dim > 0:
                    ans[a] = [(self.getContainerName(cr)[1], 
                               self.getContainerName(self.getParentContainer(cr))[1], 
                               cr) for cr in ans[a]]
                else:
                    ans[a] = ans[a]
            path_t = [self.classIdString2type(c_id[i]) for i in range(0, len(c_id), 2)]
            path_n = [c_id[i] for i in range(1, len(c_id), 2)]
            c_path = list(zip(path_t, path_n))
            c_path.append((self.classIdString2type(t), name))
            ans['#path#'] = c_path
            ans['#oid#'] = c_id[-1] # special internal attribute
            ans['name'] = name
            res.append(ans)
        return res

    def getDeletedSubContainersListByPath(self, project_name, path):
        '''Get attributes of sub-containers designated by path.
        Path is a sequence of pairs (Type, name).
        '''
        pr_id = self.getProjectByName(project_name)
        self.auth.checkPermissions(pr_id, Authorities.ACCESS_PROJ)
        assert len(path) > 0, 'Zero-length path'
        select_part = "SELECT "
        for i in range(len(path) - 1):
            select_part += "p%d.ContainerType, p%d.ContainerName, " % (i, i)
        select_part += " p%d.CodeContainer \n" % (len(path) - 1)
        from_part = " FROM " 
        where_part = "WHERE  \n"
        ind = 0
        for c in path:
            if ind == 0:
                from_part += " Containers p%d" % ind
            else:
                from_part += ", Containers p%d" % ind
            assert len(c) > 0, '(Type, Name) pairs must have Type component.'
            if ind:
                where_part += " AND "
            if ind == (len(path) - 1):
                where_part += " p%d.Status='Deleted' AND " % ind
            else:
                where_part += " p%d.Status='Actual' AND " % ind
            where_part += " p%d.ContainerType='%s'  AND p%d.TopParent=%d " % (ind, self.type2classIdString(c[0]), ind, pr_id)
            if len(c) > 1:
                if type(c[1]) == list:
                    if c[1]:  # empty list considered as no constraint!
                        where_part += " AND  p%d.ContainerName IN ('" % ind
                        where_part += "', '".join(c[1])
                        where_part += "') "
                else:
                    where_part += " AND  p%d.ContainerName='%s' " % ( ind, c[1])
            if ind == 0:
                where_part += " AND p%d.LinkUp=%d \n" % (ind, pr_id)
            else:
                where_part += " AND p%d.LinkUp=p%d.CodeContainer \n" % (ind, ind-1)
            ind += 1
        sql = select_part + from_part + '\n' + where_part
        n = self.c.execute(sql.encode('utf8'))
        tmp = self.c.fetchall()
        if not n:
            return []
        res = []
        for c_id in tmp:
            ans = {}
            t, name = self.getContainerName(c_id[-1], False)
            # replace references with type and name of referenced container
            path_t = [self.classIdString2type(c_id[i]) for i in range(0, len(c_id), 2)]
            path_n = [c_id[i] for i in range(1, len(c_id), 2)]
            c_path = list(zip(path_t, path_n))
            c_path.append((self.classIdString2type(t), name))
            ans['#path#'] = c_path
            ans['#oid#'] = c_id[-1] # special internal attribute
            ans['name'] = name
            ans['#log#'] = self.getChaneLogByOid(c_id[-1], True)
            res.append(ans)
        return res


    def getChaneLogByOid(self, oid, limit_last_operation = False, operation = None):
        """Return set of operations performed on container corresponding to oid.
        """
        assert operation in [None, 'Delete', 'Create', 'Update', 'Rename', 'Restore'], "Invalid operation requested for container %d" % oid
        sql = "SELECT u.UserName, c.Modified, c.Operation FROM  ChangeLog c LEFT JOIN Users AS u ON c.UserID = u.UserID  WHERE c.Link = %d AND c.TableType = 'Containers' " % oid
        if operation is not None:
            sql += " AND operation = '%s' " % operation
        if limit_last_operation:
            sql += ' order by c.Modified desc limit 1 '
        self.c.execute(sql)
        res = self.c.fetchall()
        return res


# =========== End of class P4DBbase


def test(fname, project, field):
    "Test triver"
    pass

def usage():
    print("""Usage: TBD
          """)
    sys.exit(1)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        usage()
    fname = sys.argv[1]
    project = sys.argv[2]
    field = sys.argv[3]

    test(fname, project, field)
