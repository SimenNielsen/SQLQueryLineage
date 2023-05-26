FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY ../../SQLQueryLineage ./SQLQueryLineage
COPY ../../SQLQueryLineageWebAPI ./SQLQueryLineageWebAPI