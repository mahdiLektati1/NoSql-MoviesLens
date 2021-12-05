import os, shutil

# Navigate to project location
os.chdir(r"C:\Users\mahdi\OneDrive\Bureau\Dev\Univ\Projet MongoDB\apache-tomcat-9.0.54\webapps")

# Delete old files
shutil.rmtree("MovieRecommender", ignore_errors=True)
os.remove("MovieRecommender.war")

# Deploy application
os.chdir(r"C:\Users\mahdi\OneDrive\Bureau\Dev\Univ\Projet MongoDB\MovieRecommender")
os.system("mvn clean install tomcat7:deploy")
