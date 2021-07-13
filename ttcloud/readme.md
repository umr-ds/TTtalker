# ttcloud impersonator

1. Install project to python-path
    - You might want to use a virtualenv
    - You might also want to do an editable install with `pip3 install -e .`
2. Install development dependencies with `pip3 install -r requirements.txt`
3. Install git pre-commit hook with `pre-commit install`
4. For testing: In the project root run `pytest --cov=ttcloud`


  
  
  
## DRAGINO HARDWARE MODIFICATION
https://github.com/dragino/Lora/blob/master/Lora_GPS%20HAT/v1.4/Lora%20GPS%20%20HAT%20for%20RPi%20v1.4.pdf
* Lora CHIP DIO3 - Pin 11 -> GPIO21 (PIN 40) 
* PIN 22 & 24 Jumper (GPIO 25 -> 8)