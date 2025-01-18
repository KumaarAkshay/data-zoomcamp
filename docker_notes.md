# Linux Basics

- `. :` Current directory
- `.. :` Parent directory
- `- :` Single-character flag (e.g., -p, -e)
- `-- :` Multi-character flag (e.g., --name, --network)
- Use `""` for names with special characters (e.g., spaces, `@`).

---

## Building Docker Images

- **Build from the current directory:**  
  `docker build .`
- **List all images:**  
  `docker image ls`
- **Run an image:**  
  `docker run image_id`
- **Stop a container:**  
  `docker stop container_name`

---

## Image Management

1. **Name an image during build:**  
   `docker build -t my_app:01 .`  
   Format: repository_name:tag_name
2. **Remove an image:**  
   `docker rmi image_name:tag`
3. **Pull an image from Docker Hub:**  
   `docker pull image_name:tag`
4. **Build with custom Dockerfile path:**
   - Example: `docker build -f configs/Dockerfile -t my_app:latest .`

---

## Container Management

1. **Run with port binding:**  
   `docker run -p host_port:container_port image_id`
2. **Detach mode:**  
   `docker run -d -p 3001:3000 image_id`
3. **Remove containers:**  
   `docker rm container_name1 container_name2`
4. **Auto-remove after stop:**  
   `docker run -d --rm image_id`
5. **Assign a name:**  
   `docker run -d --rm --name my_app -p 3001:3000 image_id`

---

## Interactive Mode

- **Run in interactive mode:**  
  `docker run -it image_id`
- **Start an existing container:**  
  `docker start container_name`

---

## Volumes

1. **Create and manage volumes:**
   - Create: `docker volume create volume_name`
   - List: `docker volume ls`
2. **Attach a managed volume:**  
   `docker run -it --rm --name container_name -v volume_name:/container_path image_name`
3. **Mount local directory:**  
   `docker run -v local_path:/container_path --rm image_name`

---

## Networking

1. **Create a network:**
   - Default bridge: `docker network create my-net`
   - Specify type: `docker network create my-net -d bridge`
2. **Run with a specific network:**  
   `docker run -it --network my-net image_id`
3. **Inspect networks:**  
   `docker network inspect network_name`

---

## Tips

- Use container names instead of IPs for easier access within the same network.
- YAML-based configurations create their own default bridge network.

---

## Docker Compose

1. **Compose commands:**
   - Start/Stop: `docker compose up` / `docker compose down`
   - Detach mode: `docker compose up -d`
   - Rebuild: `docker compose up --build`
2. **Remove networks/volumes:**  
   `docker compose down -v`
3. **Run existing containers:**  
   `docker compose start`

---

## Best Practices

1. Use a `.dockerignore` file to exclude unnecessary files.
2. Apply multi-stage builds to reduce image size.
3. Place frequently changed files at the end of the Dockerfile to optimize caching.
