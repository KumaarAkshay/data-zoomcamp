## Important Linux Commands

## Remove Commands:

- `rm file`: Deletes a file.
- `rm -d empty_directory`: Deletes an empty directory.
- `rm -r directory`: Deletes a directory and its contents.`Best Practice`
- `rm -rf directory`: Forcefully deletes a directory and everything inside it. `Imp`
- `sudo rm -rf directory`: Forcefully deletes a directory with root privileges (use with caution!).
- `mkdir downloads`: Create a directory.
- `rm yellow*`: Deletes an files start with yellow. `Imp`

---
## Path Commands:

Linux uses special symbols for path navigation:  

| Symbol | Meaning |
|---------|---------|
| `/` | Root directory |
| `.` | Current directory |
| `..` | Parent directory (one level up) |
| `~` | Home directory of the current user (`/home/username`) |
| `-` | Previous directory |

### **Example Usage:**
```bash
cd ../main_folder       # Move one level up
cd ~/docs   # Go to "docs" inside the home directory
cd -        # Switch to the previous directory
cat ./file.txt  # print file.txt in the current directory
cp ../hello.txt sub_folder/fol.txt # copy file in main folder to file in sub folder
```

---

### **Nano Editer Usage:**
```bash
nano test.txt   # Open file in editor
# Ctrl +V  to copy text
# Ctrl O -> Ctrl X  to save changes in file
```

---

## Important Commands:

- **Unzip zip file to csv:**  
  `gunzip -c fhv_tripdata_2020-02.csv.gz > file.csv`
- **Give all access to file/folder and all subfolders:**
  `sudo chmod -R 777 data_postgres`
- **Give all access to file/folder:**  
  `sudo chmod 777 data_postgres`
- **View History:**  
  `history`
- **Check OS Details:**  
  `cat /etc/os-release`
- **view all environment variable:**
  `env`
- **view environment variable with specific name:**
  `env | grep search_value`
- **Doenload a file using wget:**
  `wget -O sample_files/data.csv "https://cdn.wsform.com/wp-content/uploads/2020/06/color_srgb.csv"`
- **Difference btw -O vs -o:**
  `-O use to dowonload file content in shared path and -o saves logs in file`
- **Difference btw /folder/file.csv vs folder/file.csv:**
  `/ before folder means absolute path from root and without / means rlative path from pwd`
- **Download using curl:**
  `curl -o sample_files/datacurl.csv "https://cdn.wsform.com/wp-content/uploads/2020/06/color_srgb.csv"`
  `curl -o works as wget -O`

---

## SSH to VM

- **GCP SSH Article:**  
  `https://cloud.google.com/compute/docs/connect/create-ssh-keys`
- **Generate keys in ssh folder in linux:**  
  `ssh-keygen -t rsa -f ~/.ssh/gcpv -C akshay_vm`
- **Go to ssh floder:**  
  `cd ~/.ssh`
- **SSH folder path in linux:**
  `home/ssh`
- **SSH to vm:**
  `ssh -i ~/.ssh/gcpv akshay_vm@34.131.139.247`

---

## Terraform Notes

- **Format Terraform Files:**  
  `terraform fmt`
- **Initialize Terraform:**  
  `terraform init`
- **Plan Terraform:**  
  `terraform plan`
- **Apply Terraform:**  
  `terraform apply`
- **Apply Terraform:**  
  `terraform destroy`

---