import groovy.yaml.YamlSlurper

def configFile = new File(
        project.basedir,
        'src/main/resources/application.yml'
)

if (!configFile.exists()) {
    throw new IllegalStateException(
            "application.yml not found: ${configFile}"
    )
}

def config = new YamlSlurper().parse(configFile)
def db = config.spring?.datasource

if (!db?.url || !db?.username || db.password == null) {
    throw new IllegalStateException(
            'Missing database configuration in application.yml'
    )
}

// jOOQ configuration
project.properties.setProperty('db.url', db.url.toString())
project.properties.setProperty('db.user', db.username.toString())
project.properties.setProperty('db.password', db.password.toString())

// Flyway configuration
project.properties.setProperty('flyway.url', db.url.toString())
project.properties.setProperty('flyway.user', db.username.toString())
project.properties.setProperty('flyway.password', db.password.toString())

// Convert Spring classpath location to Maven filesystem location
def location = config.spring?.flyway?.locations

if (!(location instanceof String) ||
        !location.startsWith('classpath:')) {
    throw new IllegalStateException(
            'Expected a single classpath: Flyway location'
    )
}

def relativePath = location.substring('classpath:'.length())

def migrationDir = new File(
        project.basedir,
        "src/main/resources/${relativePath}"
)

project.properties.setProperty(
        'flyway.locations',
        'filesystem:' + migrationDir.absolutePath.replace('\\', '/')
)

log.info('Database configuration loaded from application.yml')