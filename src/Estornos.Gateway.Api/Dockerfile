FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

# Copy csproj and restore dependencies
COPY src/Estornos.Gateway.Api/*.csproj ./src/Estornos.Gateway.Api/
RUN dotnet restore src/Estornos.Gateway.Api/Estornos.Gateway.Api.csproj

# Copy everything else and build
COPY src/Estornos.Gateway.Api/ ./src/Estornos.Gateway.Api/
RUN dotnet publish src/Estornos.Gateway.Api/Estornos.Gateway.Api.csproj -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app/out .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Estornos.Gateway.Api.dll"]
