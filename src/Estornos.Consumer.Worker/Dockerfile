FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

# Copy csproj and restore dependencies
COPY src/Estornos.Consumer.Worker/*.csproj ./src/Estornos.Consumer.Worker/
RUN dotnet restore src/Estornos.Consumer.Worker/Estornos.Consumer.Worker.csproj

# Copy everything else and build
COPY src/Estornos.Consumer.Worker/ ./src/Estornos.Consumer.Worker/
RUN dotnet publish src/Estornos.Consumer.Worker/Estornos.Consumer.Worker.csproj -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app/out .
ENTRYPOINT ["dotnet", "Estornos.Consumer.Worker.dll"]
