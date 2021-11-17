# InternetMemes
Course project in Data Engineering

Members: Egle Saks, Kristjan Lõhmus, Marilin Moor, Renata Siimon

## 1. Setup

Prerequisites: 
* Docker desktop installed and running (https://www.docker.com/products/docker-desktop)
* Git installed (https://git-scm.com/downloads)
* Private ssh key added to github (https://medium.com/chaya-thilakumara/tortoisegit-how-to-create-and-upload-your-public-key-to-github-884b7b619329)

Steps:
* Open/navigate to the folder you want to add your project to and open terminal
* Run `git clone git@github.com:RSiimon/InternetMemes.git`
* Open the project in your favorite IDE
* Run `docker-compose up`
* Navigate to `localhost:8080`
* Login with `airflow` & `airflow` credentials
* Have fun

## 2. Tenative project plan

We're planning to do the project with sprints, each one has its own deadline and a project owner:
0. Project setup (16.11) - Kristjan
1. Data ingestion (16.11) - Kristjan
2. Data cleansing (28.11) - Egle
3. Create graph and relational schemas (30.11)
4. Data wrangling/transformations (05.12) - Renata
5. Data enrichment / NLP / Image processing (12.12) - Renata
6. Relational model creation (19.12) - Kristjan
7. Streaming pipelines via kafka - ?
8. Graph model creation (26.12) - Marilin
9. Presentation / example queries (31.12/03.01) - All peeps

## 3. Code & workflow conventions
* When doing something new create a pull request and commit there:
`git checkout -b 1-branch-name`,`git add -A`,`git commit -m "Commit message"`, `git push`
* Before commiting make sure to close your docker image, else it keeps writing to logs and blocks git commands
* No spaghetti code
* TBD