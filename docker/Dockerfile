# Single image, both entry points. Compose picks which one to run via `command`.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

# Restore against project files alone so dependency layers survive source edits.
COPY Directory.Build.props ./
COPY src/Shared/Shared.csproj src/Shared/
COPY src/Worker/Worker.csproj src/Worker/
COPY src/LoadGenerator/LoadGenerator.csproj src/LoadGenerator/
RUN dotnet restore src/Worker/Worker.csproj && dotnet restore src/LoadGenerator/LoadGenerator.csproj

COPY src/ src/
RUN dotnet publish src/Worker/Worker.csproj -c Release -o /app --no-restore \
 && dotnet publish src/LoadGenerator/LoadGenerator.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./

# The SDK's Prometheus exporter listens here; see METRICS_BIND_ADDRESS.
EXPOSE 9464
ENTRYPOINT ["dotnet"]
CMD ["Worker.dll"]
