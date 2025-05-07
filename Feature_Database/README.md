# Setup

Download POMD data files: https://drive.google.com/drive/folders/1nNUH2Q8y6ewYxQQNx8DOByCX13CuWbLx?usp=drive_link

# Build

    docker build -t featuredb .

# Usage

    docker run -v ./:/shared -ti featuredb