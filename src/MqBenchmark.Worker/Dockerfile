FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project files and restore
COPY src/MqBenchmark.Core/MqBenchmark.Core.csproj MqBenchmark.Core/
COPY src/MqBenchmark.Implementations/MqBenchmark.Implementations.csproj MqBenchmark.Implementations/
COPY src/MqBenchmark.PgMq.Client/MqBenchmark.PgMq.Client.csproj MqBenchmark.PgMq.Client/
COPY src/MqBenchmark.Worker/MqBenchmark.Worker.csproj MqBenchmark.Worker/
RUN dotnet restore MqBenchmark.Worker/MqBenchmark.Worker.csproj

# Copy source and publish
COPY src/MqBenchmark.Core/ MqBenchmark.Core/
COPY src/MqBenchmark.Implementations/ MqBenchmark.Implementations/
COPY src/MqBenchmark.Worker/ MqBenchmark.Worker/
COPY src/MqBenchmark.PgMq.Client/ MqBenchmark.PgMq.Client/
RUN dotnet publish MqBenchmark.Worker/MqBenchmark.Worker.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "MqBenchmark.Worker.dll"]
