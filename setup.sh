#!/bin/sh

# Remove obsolete credential file
rm -f ~/.aws/config
 
# Install of Oracle Java JDK
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get -y update
sudo apt-get -y install oracle-java7-installer

# Install sbt
wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb
sudo dpkg -i sbt-0.13.7.deb
rm sbt-0.13.7.deb

# Make sure we have enough memory for Spark
export _JAVA_OPTIONS="-Xmx20g"
