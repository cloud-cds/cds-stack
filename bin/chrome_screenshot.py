# Install chromedriver from https://sites.google.com/a/chromium.org/chromedriver/downloads

import os

from optparse import OptionParser

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

CHROME_PATH = '/usr/bin/google-chrome'
CHROMEDRIVER_PATH = '/usr/local/bin/chromedriver'
WINDOW_SIZE = "1920,1920"

chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
chrome_options.binary_location = CHROME_PATH

class element_has_css_class(object):
  """An expectation for checking that an element has a particular css class.

  locator - used to find the element
  returns the WebElement once it has the particular css class
  """
  def __init__(self, locator, css_class):
    self.locator = locator
    self.css_class = css_class

  def __call__(self, driver):
    element = driver.find_element(*self.locator)   # Finding the referenced element
    if self.css_class in element.get_attribute("class"):
        return element
    else:
        return False

def make_screenshot(url, output):
    if not url.startswith('http'):
        raise Exception('URLs need to start with "http"')

    driver = webdriver.Chrome(
        executable_path=CHROMEDRIVER_PATH,
        chrome_options=chrome_options
    )
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    element = wait.until(element_has_css_class((By.ID, 'loading'), "done"))
    driver.save_screenshot(output)
    driver.close()

if __name__ == '__main__':
    usage = "usage: %prog [options] <url> <output>"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) < 2:
        parser.error("please specify a URL and an output")

    make_screenshot(args[0], args[1])