from geopy import geocoders

gn = geocoders.GeoNames()
geolocator = Nominatim(user_agent='SAMC')
gn.geocode('ANCHORAGE, AK')
