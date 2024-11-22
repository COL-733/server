import tkinter as tk

class ServerGUI():

    def __init__(self, name, shutdown):
        self.r = tk.Tk()

        self.r.title(name)
        button = tk.Button(self.r, text='Shut Down', width=25, command=shutdown)
        button.pack()
    
    def mainloop(self):
        self.r.mainloop()
    
    def exit(self):
        self.r.destroy()