import re
from atlassian import Confluence
import os
import requests
import json
from prefect import flow, task

  
def get_attachments(pageid, title, c, session):
    attachments_container = c.get_attachments_from_content(pageid, start=0, limit=50, expand=None, filename=None, media_type=None)
    
    path = "attachments_images/" + title 
    isExist = os.path.exists(path)

    if not isExist:
        os.makedirs(path)
    
    attachments = attachments_container['results']

    temp_fname = ''
    for attachment in attachments:
        fname = attachment['title']
        if fname == temp_fname:
            continue

        download_link =  c.url + attachment['_links']['download'] 
        r = session.get(download_link)

        if r.status_code == 200:
            with open("attachments_images/" + title+"/"+fname, "wb") as f:
                for bits in r.iter_content(): 
                    f.write(bits)

def get_confluence_docs_parent(parent_page_id, c, master_page_id_list,master_page_titles_list):
    parent_page_id = parent_page_id[0]
    child_pages_id_temp = c.get_child_id_list(parent_page_id, type="page", start=None, limit=1000) 
    ''' If parent page id passed does not have any children, exit the function (recursion check)'''
    if not child_pages_id_temp:
        return

    child_pages_titles_temp = c.get_child_title_list(parent_page_id, type="page", start=None, limit=1000)

    ''' If statement avoids having a child that is now a parent entered into the master list twice'''
  
    if parent_page_id not in master_page_id_list:
        parent_page_title = c.get_page_by_id(parent_page_id, expand=None) ['title']
        master_page_id_list.insert(0,parent_page_id)
        master_page_titles_list.insert(0,parent_page_title)

    ''' Append returned child pages to the master, global lists. '''
    for child_id, child_title in zip (child_pages_id_temp, child_pages_titles_temp): 
        master_page_id_list.append(child_id)
        master_page_titles_list.append(child_title)

    '''Recursion to drill down into each child ID to grab any child pages of a child page'''
    for id in child_pages_id_temp: 
        id = [id]
        get_confluence_docs_parent (id,c,master_page_id_list,master_page_titles_list)


    ''' Recursion ends, loop through master list of titles and ids '''
    for index, pageid in enumerate (master_page_id_list):
        title = master_page_titles_list [index]
        title = title.replace(':','-')
        title = title.replace('/','')
        title = title.replace('<', '_')
        title = title.replace('>', '_')
        title = title.replace('?','')
        title = title.replace('\\','')
        title = title.replace('','')
        html = c.get_page_by_id(pageid, expand='body.storage', status=None, version=None) 
        html = html['body']['storage']['value']

        with open(''+ title +'.txt', 'wb') as f:
            f.write(html.encode())
        
        get_attachments(pageid, title, c)

def get_confluence_docs_no_parent(pageid_list,c):
    '''For each pageid in csv''' 

    title = c.get_page_by_id(pageid, expand=None) ['title']
    for index, pageid in enumerate(pageid_list):
        title = title.replace(':','-') 
        title = title.replace('/','')
        title = title.replace('<', '_')
        title = title.replace('>', '_')
        title = title.replace('?', '_')
        title = title.replace('\\','') 
        title = title.replace('*', '_')


    html = c.get_page_by_id(pageid, expand='body.storage', status=None, version=None) 
    html = html['body']['storage']['value']

    with open('html_raw/'+ title +' .txt', 'wb') as f:
        f.write(html.encode())

    get_attachments(pageid, title, c)


def get_confluence_docs_pagetitles(pagetitle_list, spacename,c):
    ''' For each title in csv ''' 
    for index, title in enumerate (pagetitle_list): 
        pageid = c.get_page_id(spacename,title)
        html = c.get_page_by_id(pageid, expand='body.storage', status=None, version=None) 
        html = html['body']['storage']['value']
        title = title.replace(':','')
        title = title.replace('/','')
        title = title.replace('<', '_')
        title = title.replace('>', '_')
        title = title.replace('?','')
        title = title.replace('\\', '_')
        title = title.replace('*','')

        with open('html_raw/'+ title + '.txt', 'wb') as f:
            f.write(html.encode())
        get_attachments (pageid, title, c)


def get_confluence_docs_space(space_name, c, session):
    all_pages = c.get_all_pages_from_space(space_name, start=0, limit=1000, status=None, expand=None, content_type='page')
    ''' For each pageid in csv '''

    for page in all_pages:
        pageid = page['id']

        title = c.get_page_by_id(pageid, expand=None)['title']
        title = title.replace(':','-')
        title = title.replace('/','')
        title = title.replace('<','')
        title = title.replace('>','')
        title = title.replace('?','')
        title = title.replace('\\','')
        title = title.replace('','')

        html = c.get_page_by_id(pageid, expand = 'body.storage', status=None, version=None)
        html = html['body' ][ 'storage']['value']
        with open('html_raw/'+ title + '.txt', 'wb') as f: 
            f.write(html.encode())
        
        print(">>>> Page Title: {}".format(title))
        get_attachments(pageid, title, c, session)


def norm_hrefs (html):
    '''Takes a single html string and replace hrefs with url'''

    newstring = ''
    start = 0
    for m in re.finditer (r' (<a href.*?</a>)', html):
        end, newstart = m. span()
        newstring + html [start: end] 
        url = re.search(r' href=[\'"]?([^\'">]+)', m.group(0)) 
        new_url = url.group(0).replace('href="','')
        newstring += new_url
        start = newstart
    
    newstring += html[start:] 
    return newstring


def set_dirs():
    dir_to_create = ['attachments_images', 'html_raw', 'html']
    for dirs in dir_to_create:
        isExist = os.path.exists('{}'.format(dirs))

        if not isExist:
            print(">>>> Creating Directories")
            os.makedirs('{}'.format(dirs))

def norm_html(html):
    html = " ".join(html.split())
    html = html.replace('\\','')
    html = html.replace('"',"'")
    html = re.sub('<img src.*?>', '', html)
    html = re.sub('<img alt.*?>', '', html) 
    html = re.sub('<a id.*?a>', '', html)
    html = re.sub('<ac:."?>', '', html) 
    html = re.sub('</ac:.*?>', '', html)
    return html

def save_as_txt (html, filename): 
    with open('html/' + filename + '.txt', 'wb') as f: 
        f.write(html.encode('utf-8'))

'''Convert all docx documents to html and normalize images and whitespace'''

def full_norm_html():
    directory = 'html_raw'
    for filename in os.listdir (directory):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            ext = os.path.splitext(f)[-1].lower()
            if ext == '.txt':
                with open('html_raw/' + filename, "rb") as f:
                    html = f.read()
                    html = html.decode()
                    filename = filename.replace(ext, '')
                    html = norm_html(html)
                    html = norm_hrefs(html)
                    save_as_txt(html,filename)

def get_docs_based_input(c, s):
    with open('inputs.json') as json_file:
        data = json.load(json_file)

    space_name = data['SPACE_NAME']
    input_list = data['INPUTS']
    input_type = data['INPUT_TYPE'] 
    
    if input_type == 'parent':
        master_page_id_list = [] 
        master_page_titles_list = []
        get_confluence_docs_parent (input_list,c, master_page_id_list, master_page_titles_list)

    elif input_type == 'ids':
        get_confluence_docs_no_parent (input_list,c) 

    elif input_type == 'space':
        get_confluence_docs_space(space_name, c, s)
    
    elif input_type == 'titles':
        get_confluence_docs_pagetitles (input_list, space_name, c)
    else:
        print("Incorrect input")
        quit()
    return 'No SP Site'


def get_confluence_session():
    api_key = ''
    api_token = ''
    s = requests.Session()
    s.headers['Authorization'] = full
    c = Confluence(url='',session=s)
    return c, s


def get_file_dict():
    '''gets all files and paths for uploading to SharePoint Site'''
    file_dict  = {}

    for index, (dirpath, dirnames, filenames) in enumerate(os.walk ('attachments_images')) :
        if index == 0:
            continue

    filename_paths = []
    for file in filenames:
        filename_paths.append(os.path.join(os.path.abspath (dirpath), file))

    dirpath = dirpath.replace('attachments_images\\','')
    dirpath = dirpath.replace('attachments_images/','') 
    file_dict['Confluence_Files/{}'.format (dirpath.replace('\\','/'))] = filename_paths

    for index, (dirpath, dirnames, filenames) in enumerate (os.walk('html')):
        filename_paths = []
        for file in filenames:
            filename_paths.append(os.path.join(os.path. abspath(dirpath), file))

        file_dict['Confluence_Files/{}'.format(dirpath.replace('\\','/'))] = filename_paths

    return file_dict

@flow
def main():
    c, s  = get_confluence_session()
    set_dirs()
    print(">>>> CREATED DIRECTORIES""")
    
    spsite = get_docs_based_input(c,s)
    full_norm_html()

    
