import time

try:
    import board
    import busio
    import neopixel_spi as neopixel
except Exception as e:
    print("Import error:", e)
    print("Install required packages first.")
    raise

NUM_PIXELS = 15
BRIGHTNESS = 0.2   # keep low for testing

def main():
    try:
        print("Initializing SPI LED strip...")
        spi = busio.SPI(board.SCK, MOSI=board.MOSI)

        pixels = neopixel.NeoPixel_SPI(
            spi,
            NUM_PIXELS,
            auto_write=False,
            pixel_order=neopixel.GRB,
            frequency=6400000
        )

        pixels.brightness = BRIGHTNESS

        # OFF first
        pixels.fill((0, 0, 0))
        pixels.show()
        time.sleep(1)

        print("Testing RED")
        pixels.fill((255, 0, 0))
        pixels.show()
        time.sleep(2)

        print("Testing GREEN")
        pixels.fill((0, 255, 0))
        pixels.show()
        time.sleep(2)

        print("Testing BLUE")
        pixels.fill((0, 0, 255))
        pixels.show()
        time.sleep(2)

        print("Testing WHITE")
        pixels.fill((255, 255, 255))
        pixels.show()
        time.sleep(2)

        print("Chasing one LED...")
        pixels.fill((0, 0, 0))
        pixels.show()
        for i in range(NUM_PIXELS):
            pixels.fill((0, 0, 0))
            pixels[i] = (255, 255, 0)
            pixels.show()
            time.sleep(0.2)

        print("Turning OFF")
        pixels.fill((0, 0, 0))
        pixels.show()

        print("LED test finished.")

    except Exception as e:
        print("LED test failed:", e)

if __name__ == "__main__":
    main()
