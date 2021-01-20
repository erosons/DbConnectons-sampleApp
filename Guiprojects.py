# Graphic module impor
from tkinter import Tk, Button, Label, Scrollbar, Listbox, StringVar, Entry, W, E, S, N, END
from tkinter import ttk
from tkinter import messagebox
# connect configuration to database
from mysqlconfig import dbConfig
import pymysql

conn = pymysql.connect(**dbConfig)
print(conn)
cur = conn.cursor()


class mylibrary:
    def __init__(self):
        self.conn = pymysql.connect(**dbConfig)
        self.cur = conn.cursor()
        print("You are connected to the database")
        print(conn)

    def __del__(self):
        self.conn.close()

    def view(self):
        self.cur.execute("SELECT*FROM MYBOOKAPP")
        rows = self.cur.fetchall()
        return rows

    def insert(self, title, author, isbn):
        sql = ("INSERT INTO MYBOOKAPP(title,author,isbn) VALUES(%s,%s,%s)")
        values = [title, author, isbn]
        self.cur.execute(sql, values)  # print("Rows inserted")
        self.conn.commit()
        messagebox.showinfo(title="Book Database",
                            message="New Book added to database")

    def Update(self, id, title, author, isbn):
        tsql = 'UPDATE MYBOOKAPP SET title=%s,author=%s,isbn=%s Where id=%s'
        # print("Rows Update")
        self.cur.execute(tsql, [title, author, isbn, id])
        self.conn.commit()
        messagebox.showinfo(title="Book Database", message="Book update")

    def delete(self, id):
        delsql = 'DELETE FROM MYBOOKAPP  Where id=%s'
        self.cur.execute(delsql, [id])  # print("delete Update")
        self.conn.commit()
        messagebox.showinfo(title="Book Database", message="Book Deleted")


db = mylibrary()
# Event populate when the list box is selected


def get_selected_row(event):
    global selected_tuple
    index = List_bx.curselection()[0]
    selected_tuple = List_bx.get(index)
    title_entry.delete(0, 'end')
    title_entry.insert('end', selected_tuple[1])
    author_entry.delete(0, 'end')
    author_entry.insert('end', selected_tuple[2])
    isbn_entry.delete(0, 'end')
    isbn_entry.insert('end', selected_tuple[3])

# clearing the List box and and inserting into


def view_records():
    List_bx.delete(0, 'end')
    for row in db.view():
        List_bx.insert('end', row)


# add a book to my libray function
def add_book():
    db.insert(title_text.get(), author_text.get(), isbn_text.get())
    List_bx.delete(0, 'end')
    List_bx.insert(
        'end', (title_text.get(), author_text.get(), isbn_text.get()))
    title_entry.delete(0, 'end')  # clears input after inserting
    author_entry.delete(0, 'end')  # clears input after inserting
    isbn_entry.delete(0, 'end')  # clears input after inserting
    conn.commit()

# records delete


def delete_records():
    db.delete(selected_tuple[0])
    conn.commit()
# clear Screen


def clear_screen():
    List_bx.delete(0, 'end')    # clears Listbox
    title_entry.delete(0, 'end')  # clears title entry
    author_entry.delete(0, 'end')  # clears author entry
    isbn_entry.delete(0, 'end')  # clears isbn entry


def update_records():
    db.Update(selected_tuple[0], title_text.get(),
              author_text.get(), isbn_text.get())
    title_entry.delete(0, 'end')  # clears title entry
    author_entry.delete(0, 'end')  # clears author entry
    isbn_entry.delete(0, 'end')  # clears isbn entry
    conn.commit()

# Exit the application


def on_closing():
    dd = db
    if messagebox.askokcancel("Quit", "Do you want quit?"):
        root.destroy()
        del dd


# Background
root = Tk()  # Create application window
root.title("MY BOOK LIBRARY")  # Add title to the application
root.configure(background="Khaki")
root.geometry("850x500")
root.resizable(width=False, height=False)
# Input box where you can query the database by author
title_label = ttk.Label(
    root, text="Title", background="light green", font=("TkDefaultFont", 16))
title_label.grid(row=0, column=0, sticky=W)
title_text = StringVar()
title_entry = ttk.Entry(root, width=24, textvariable=title_text)
title_entry.grid(row=0, column=1, sticky=W)
# Input box where you can query the database by author
author_label = ttk.Label(
    root, text="Author", background="light green", font=("TkDefaultFont", 16))
author_label.grid(row=0, column=2, sticky=W)
author_text = StringVar()
author_entry = ttk.Entry(root, width=24, textvariable=author_text)
author_entry.grid(row=0, column=3, sticky=W)
# Input box where you can query the database by isbn
isbn_label = ttk.Label(
    root, text="ISBN", background="light green", font=("TkDefaultFont", 16))
isbn_label.grid(row=0, column=4, sticky=W)
isbn_text = StringVar()
isbn_entry = ttk.Entry(root, width=24, textvariable=isbn_text)
isbn_entry.grid(row=0, column=5, sticky=W)
# Add Button
add_btn = Button(root, text="Add Book", bg="Red", fg="White",
                 font="helvetica 10 bold", command=add_book)
add_btn.grid(row=0, column=6, sticky=W)
# Listbox
List_bx = Listbox(root, height=20, width=40,
                  font="helvetica 10 bold", bg="light blue")
List_bx.grid(row=3, column=1, columnspan=18, sticky=W+E, pady=40, padx=15)
List_bx.bind('<<ListboxSelect>>', get_selected_row)

# scroll bar
scroll_bar = Scrollbar(root)
scroll_bar.grid(row=1, column=8, rowspan=14, sticky=W)

# Listbox confirugration
List_bx.configure(yscrollcommand=scroll_bar.set)
scroll_bar.configure(command=List_bx.yview)

# View Button
View_btn = Button(root, text="View ALL Records", bg="Red",
                  fg="White", font="helvetica 10 bold", command=view_records)
View_btn.grid(row=15, column=2)

# modify Button
Modify_btn = Button(root, text="Modify Records", bg="Red",
                    fg="White", font="helvetica 10 bold", command=update_records)
Modify_btn.grid(row=15, column=1)

# Delete Button
delete_btn = Button(root, text="Delete Records", bg="Red",
                    fg="White", font="helvetica 10 bold", command=delete_records)
delete_btn.grid(row=15, column=3)

# clear Button
Clear_btn = Button(root, text="Clear Screen", bg="Red",
                   fg="White", font="helvetica 10 bold", command=clear_screen)
Clear_btn.grid(row=15, column=4)

# Exit Application Button
exit_btn = Button(root, text="Exit Application", bg="Red",
                  fg="White", font="helvetica 10 bold", command=root.destroy)
exit_btn.grid(row=15, column=5)

root.mainloop()
