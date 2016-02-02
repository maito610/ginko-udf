# Ginko Pig UDF Project

## Preparation of env

1. Install Java 7 or 8

Check if java is installed: `java -version`

If it is not installed go to [Oracle's download page][http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html]

Ubuntu:
```
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
```
2. Install maven

MacOSX
```
brew install maven
```
Ubuntu:
```
sudo apt-get install maven
```
or manually
```
wget http://ftp.kddilabs.jp/infosystems/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
tar zxvf apache-maven-3.3.9-bin.tar.gz
mv apache-maven-3.3.9-bin apache-maven-3.3.9
sudo mv /usr/local/opt apache-maven-3.3.9
sudo ln -s /usr/local/opt/apache-maven-3.3.9 /usr/bin/mvn
```

3. Install the XPresso dependency from GitHub
```
git clone https://github.com/WantedTechnologies/xpresso
cd xpresso
mvn clean install
```

4. Build and package the project
From the project folder:
```
mvn clean package
```

This will download all dependencies, compile the sources and build the jar and copy it to `/libs` folder.


Main dependencies are:
- XPresso
- Hadoop 2 and Pig 15.0
- Linkedin Datafu
- Google Guava and AutoValue

Logging facade SLF4J is used with the java.util.logging backend.
