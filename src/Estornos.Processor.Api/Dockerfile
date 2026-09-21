FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /app

# Copy csproj and restore dependencies
COPY src/Estornos.Processor.Api/*.csproj ./src/Estornos.Processor.Api/
RUN dotnet restore src/Estornos.Processor.Api/Estornos.Processor.Api.csproj

# Copy everything else and build
COPY src/Estornos.Processor.Api/ ./src/Estornos.Processor.Api/
RUN dotnet publish src/Estornos.Processor.Api/Estornos.Processor.Api.csproj -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=build /app/out .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Estornos.Processor.Api.dll"]
