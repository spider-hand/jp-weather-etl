<script setup lang="ts">
import { importLibrary, setOptions } from '@googlemaps/js-api-loader'
import { onMounted, ref } from 'vue'

type GeoJsonFeature = {
  geometry?: {
    type?: unknown
    coordinates?: unknown
  }
  properties?: {
    station_name?: unknown
  }
}

const mapElement = ref<HTMLElement>()

const tokyoDate = (): string => {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  })
    .format(new Date())
    .replaceAll('-', '')
}

const stationFrom = (feature: GeoJsonFeature) => {
  const coordinates = feature.geometry?.coordinates
  const name = feature.properties?.station_name

  if (
    feature.geometry?.type !== 'Point' ||
    !Array.isArray(coordinates) ||
    coordinates.length < 2 ||
    !coordinates.every((coordinate) => typeof coordinate === 'number') ||
    typeof name !== 'string' ||
    !name
  ) {
    return null
  }

  const [lng, lat] = coordinates
  return Number.isFinite(lat) &&
    Number.isFinite(lng) &&
    lat >= -90 &&
    lat <= 90 &&
    lng >= -180 &&
    lng <= 180
    ? { lat, lng, name }
    : null
}

const loadDailyWeatherConditions = async (): Promise<GeoJsonFeature[]> => {
  const url = `http://localhost:9000/processed/${tokyoDate()}/daily_weather_conditions.geojson`
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`Could not load today's weather stations (${response.status})`)
  }

  const geoJson: unknown = await response.json()
  if (
    !geoJson ||
    typeof geoJson !== 'object' ||
    !('type' in geoJson) ||
    geoJson.type !== 'FeatureCollection' ||
    !('features' in geoJson) ||
    !Array.isArray(geoJson.features)
  ) {
    throw new Error("Today's weather data is not a GeoJSON FeatureCollection")
  }

  return geoJson.features
}

const initializeMap = async () => {
  const apiKey = import.meta.env.GOOGLE_MAPS_API_KEY
  if (!apiKey) {
    throw new Error('GOOGLE_MAPS_API_KEY is not configured')
  }

  setOptions({ key: apiKey, v: 'weekly' })
  const [{ Map }, { AdvancedMarkerElement }] = await Promise.all([
    importLibrary('maps'),
    importLibrary('marker'),
  ])
  const map = new Map(mapElement.value!, {
    center: { lat: 36.2, lng: 138.25 },
    zoom: 5,
    mapId: 'DEMO_MAP_ID',
  })

  const stations = (await loadDailyWeatherConditions())
    .map(stationFrom)
    .filter((station) => station !== null)
  if (!stations.length) {
    throw new Error("Today's weather data contains no valid stations")
  }

  const bounds = new google.maps.LatLngBounds()
  const labels: HTMLElement[] = []
  for (const station of stations) {
    const markerContent = document.createElement('div')
    markerContent.className = 'station-marker'

    const dot = document.createElement('span')
    dot.className = 'station-dot'
    markerContent.append(dot)

    const label = document.createElement('span')
    label.className = 'station-label'
    label.textContent = station.name
    markerContent.append(label)
    labels.push(label)

    new AdvancedMarkerElement({
      map,
      position: station,
      title: station.name,
      content: markerContent,
    })
    bounds.extend(station)
  }

  const updateLabels = () => {
    const hidden = (map.getZoom() ?? 0) < 9
    labels.forEach((label) => (label.hidden = hidden))
  }
  map.addListener('zoom_changed', updateLabels)
  map.fitBounds(bounds)
  updateLabels()
}

onMounted(async () => {
  try {
    await initializeMap()
  } catch (error) {
    console.error('Could not initialize the map', error)
  }
})
</script>

<template>
  <div ref="mapElement" class="map" aria-label="Weather observation stations"></div>
</template>

<style>
.station-marker {
  position: relative;
}

.station-dot {
  display: block;
  width: 12px;
  height: 12px;
  box-sizing: border-box;
  border: 2px solid white;
  border-radius: 50%;
  background: #1a73e8;
  box-shadow: 0 1px 3px rgb(0 0 0 / 40%);
}

.station-label {
  position: absolute;
  bottom: calc(100% + 0.25rem);
  left: 50%;
  padding: 0.2rem 0.35rem;
  transform: translateX(-50%);
  border-radius: 0.2rem;
  color: #202124;
  background: rgb(255 255 255 / 90%);
  font: 600 14px/1.2 system-ui, sans-serif;
  white-space: nowrap;
  box-shadow: 0 1px 3px rgb(0 0 0 / 30%);
}
</style>
