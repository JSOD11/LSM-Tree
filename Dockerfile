# Use an official Ubuntu base image
FROM ubuntu:latest

# Set maintainer label (optional)
LABEL maintainer="yourname@example.com"

# Install necessary packages for GSL, C++ compiler, and other utilities
RUN apt-get update && apt-get install -y \
    build-essential \       
    libgsl-dev \            
    gsl-bin \               
    cmake \                 
    git \                  
    && rm -rf /var/lib/apt/lists/*  

# Set the working directory inside the container
WORKDIR /workspace

# Default command to run on container start
CMD ["bash"]
