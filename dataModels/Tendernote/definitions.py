

import matplotlib.pyplot as plt
import matplotlib.patches as patches


class CanadianTender:

    def __init__(self, height, width):
       self.height_mm= height
       self.width_mm= width

    def plot_dark_emerald_rectangle(self, height, width):

        fig, ax = plt.subplots()
        dark_emerald_color = "#13573c"
        rectangle = patches.Rectangle((0, 0), width, height, linewidth=1, edgecolor='none', facecolor=dark_emerald_color)

        ax.add_patch(rectangle)

        # Set aspect ratio to be equal
        ax.set_aspect('equal', adjustable='box')

        # Set axis limits based on rectangle dimensions
        plt.xlim(0, self.width_mm)
        plt.ylim(0, self.height_mm)

        # Hide the axes
        plt.axis('off')

        # Show the plot
        plt.show()
    