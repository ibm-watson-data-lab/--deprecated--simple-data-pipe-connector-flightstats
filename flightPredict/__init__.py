# -------------------------------------------------------------------------------
# Copyright IBM Corp. 2016
# 
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -------------------------------------------------------------------------------

#global variable
#credentials
cloudantHost=None
cloudantUserName=None
cloudantPassword=None
sqlContext=None
weatherUrl=None

attributes=['dewPt','rh','vis','wc',
    #'wdir',
    'wspd','feels_like','uv_index']
attributesMsg = ['Dew Point', 'Relative Humidity', 'Prevailing Hourly visibility', 'Wind Chill', 
     #'Wind direction',
    'Wind Speed','Feels Like Temperature', 'Hourly Maximum UV Index']
