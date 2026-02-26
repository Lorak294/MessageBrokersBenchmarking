FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

# Copy project files and restore
COPY src/MqBenchmark.Core/MqBenchmark.Core.csproj MqBenchmark.Core/
COPY src/MqBenchmark.Orchestrator/MqBenchmark.Orchestrator.csproj MqBenchmark.Orchestrator/
RUN dotnet restore MqBenchmark.Orchestrator/MqBenchmark.Orchestrator.csproj

# Copy source and publish
COPY src/MqBenchmark.Core/ MqBenchmark.Core/
COPY src/MqBenchmark.Orchestrator/ MqBenchmark.Orchestrator/
RUN dotnet publish MqBenchmark.Orchestrator/MqBenchmark.Orchestrator.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENV ASPNETCORE_URLS=http://+:8080
ENV ASPNETCORE_ENVIRONMENT=Production
EXPOSE 8080

ENTRYPOINT ["dotnet", "MqBenchmark.Orchestrator.dll"]
