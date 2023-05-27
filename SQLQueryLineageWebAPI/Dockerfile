#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

ENV ASPNETCORE_ENVIRONMENT=Development

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["SQLQueryLineageWebAPI/SQLQueryLineageWebAPI.csproj", "SQLQueryLineageWebAPI/"]
COPY ["SQLQueryLineage/SQLQueryLineage.csproj", "SQLQueryLineage/"]
RUN dotnet restore "SQLQueryLineageWebAPI/SQLQueryLineageWebAPI.csproj"
COPY . .
WORKDIR "/src/SQLQueryLineageWebAPI"
RUN dotnet build "SQLQueryLineageWebAPI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "SQLQueryLineageWebAPI.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SQLQueryLineageWebAPI.dll"]