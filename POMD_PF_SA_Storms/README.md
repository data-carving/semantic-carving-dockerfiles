# Setup

Download POMD data files: https://drive.google.com/drive/folders/1nNUH2Q8y6ewYxQQNx8DOByCX13CuWbLx?usp=drive_link

# Build

    docker build -t pomd_pf_sa_storms .

# Usage

    docker run -v ./:/shared -ti pomd_pf_sa_storms
