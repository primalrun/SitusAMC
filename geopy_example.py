from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

lat = -27.4705
long = 153.0260

geocoder = Nominatim(user_agent='SAMC')
geo_reverse = RateLimiter(geocoder.reverse, min_delay_seconds=1)
location = geo_reverse((lat, long), language='en', exactly_one=True)
print(location.raw)
