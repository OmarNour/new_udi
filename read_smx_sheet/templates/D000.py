from selenium import webdriver
from time import sleep
import pandas as pd

# Inputs
sourceLocation = []
targetLocation = []
shortestRouteTitle = []
shortestRouteDistance = []
parallel_templates = []


def find(driver,destination, cf):
    print(destination)
    print(cf.output_folder_name)
    print(cf.output_folder_path)
    sleep(2)
    source_location = cf.source_location
    print(source_location)
    driver.get("https://www.google.com/maps/dir/" + source_location)
    minDistance = 10000
    minIndex = 0
    routeTitleCol = []
    sleep(10)
    targetLocationInput = driver.find_element_by_xpath(
        '/html/body/jsl/div[3]/div[9]/div[3]/div[1]/div[2]/div/div[3]/div[1]/div[2]/div[2]/div/div/input')
    targetLocationInput.send_keys(destination)
    sleep(10)
    searchButton = driver.find_element_by_xpath(
        '/html/body/jsl/div[3]/div[9]/div[3]/div[1]/div[2]/div/div[3]/div[1]/div[2]/div[2]/button[1]')
    searchButton.click()
    sleep(10)
    routes = driver.find_elements_by_class_name('section-directions-trip-title')
    routes_distances = driver.find_elements_by_class_name('section-directions-trip-distance')
    for routeTitle in routes:
        routeTitleText = routeTitle.text
        if routeTitleText != '':
            routeTitleCol.append(routeTitleText)
    count = 0
    for routeDistance in routes_distances:
        routeDistanceText = routeDistance.text.replace('km', '')
        routeDistanceInKM = routeDistanceText.replace('كم', '')
        minRouteDistance = float(routeDistanceInKM.strip())
        if minRouteDistance < minDistance:
            minDistance = minRouteDistance
            minIndex = count
        count = count + 1
    sourceLocation.append(source_location)
    targetLocation.append(destination)
    shortestRouteDistance.append(minDistance)
    shortestRouteTitle.append(routeTitleCol[minIndex])


def parse_file(cf):
    target_locations = pd.read_csv(cf.destination_location)
    for target_location in target_locations['Target Locations']:
        driver = webdriver.Chrome("/Users/dinaelhusseiny/Downloads/chromedriver")
        find(driver,target_location, cf)
        driver.close()
    df = pd.DataFrame(
    {'Source Location': sourceLocation,
         'Target Location': targetLocation,
         'Route Name': shortestRouteTitle,
         'Route Distance': shortestRouteDistance})

    # Extract the path from the browsed file and concatenate the outputFileName
    export_file_path = cf.output_folder_path + '/' + cf.output_folder_name + '.csv'
    print(export_file_path)
    df.to_csv(export_file_path, index=False, header=True, encoding='utf-8-sig')
