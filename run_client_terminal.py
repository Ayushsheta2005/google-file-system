#!/usr/bin/env python3
"""
Simple terminal client for GFS
Run this after starting masters and chunk servers with Docker
"""

from client import MultiMasterGFSClient
import sys

def main():
    print("=" * 60)
    print("🚀 GFS Client - Terminal Mode")
    print("=" * 60)
    
    # Connect to the 3 masters running in Docker
    master_addresses = [
        'localhost:9010',  # master1
        'localhost:9011',  # master2
        'localhost:9012'   # master3
    ]
    
    print(f"\n📡 Connecting to masters: {master_addresses}")
    
    try:
        client = MultiMasterGFSClient(master_addresses)
        print(f"✅ Connected! Current leader: {client.current_leader}")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        print("\n⚠️  Make sure Docker containers are running:")
        print("   ./start_masters.sh 3")
        print("   ./start_chunkservers.sh 3")
        sys.exit(1)
    
    # Get cluster status
    print("\n" + "=" * 60)
    print("📊 Cluster Status")
    print("=" * 60)
    try:
        status = client.get_cluster_status()
        print(f"\n🏛️  Primary Master: {status['primary_master']['master_id']}")
        print(f"   Term: {status['primary_master']['term']}")
        print(f"   Address: {status['primary_master']['address']}")
        
        print(f"\n👥 All Masters ({len(status['all_masters'])}):")
        for master in status['all_masters']:
            role = "PRIMARY ⭐" if master['is_primary'] else "BACKUP"
            print(f"   - {master['master_id']}: {role} (Term {master['term']})")
    except Exception as e:
        print(f"⚠️  Could not get cluster status: {e}")
    
    # Interactive menu
    print("\n" + "=" * 60)
    print("📁 Interactive Operations")
    print("=" * 60)
    
    while True:
        print("\n" + "-" * 60)
        print("Choose an operation:")
        print("1. Create a file")
        print("2. Write to a file")
        print("3. Read from a file")
        print("4. List all files")
        print("5. Get file info")
        print("6. Delete a file")
        print("7. Create directory")
        print("8. List directory")
        print("9. Delete directory")
        print("10. Get cluster status")
        print("0. Exit")
        print("-" * 60)
        
        choice = input("\nEnter your choice (0-10): ").strip()
        
        try:
            if choice == '0':
                print("\n👋 Goodbye!")
                break
            
            elif choice == '1':
                filename = input("Enter filename: ").strip()
                client.create_file(filename)
                print(f"✅ File '{filename}' created successfully!")
            
            elif choice == '2':
                filename = input("Enter filename: ").strip()
                content = input("Enter content to write: ").strip()
                client.write_file(filename, content.encode())
                print(f"✅ Content written to '{filename}'!")
            
            elif choice == '3':
                filename = input("Enter filename: ").strip()
                data = client.read_file(filename)
                print(f"\n📄 Content of '{filename}':")
                print("-" * 60)
                print(data.decode())
                print("-" * 60)
            
            elif choice == '4':
                files = client.list_all_files()
                print(f"\n📋 All files in system ({len(files)}):")
                if files:
                    for i, f in enumerate(files, 1):
                        print(f"   {i}. {f}")
                else:
                    print("   (No files found)")
            
            elif choice == '5':
                filename = input("Enter filename: ").strip()
                info = client.get_file_info(filename)
                print(f"\n📊 File Info for '{filename}':")
                print(f"   Size: {info.get('size', 0)} bytes")
                print(f"   Number of chunks: {len([c for c in info.get('chunks', []) if c])}")
                print(f"   Chunks: {info.get('chunks', [])}")
            
            elif choice == '6':
                filename = input("Enter filename to delete: ").strip()
                confirm = input(f"⚠️  Are you sure you want to delete '{filename}'? (yes/no): ").strip().lower()
                if confirm == 'yes':
                    client.delete_file(filename)
                    print(f"✅ File '{filename}' deleted!")
                else:
                    print("❌ Deletion cancelled")
            
            elif choice == '7':
                dirname = input("Enter directory name: ").strip()
                client.create_directory(dirname)
                print(f"✅ Directory '{dirname}' created!")
            
            elif choice == '8':
                dirname = input("Enter directory name (or / for root): ").strip()
                if not dirname:
                    dirname = '/'
                entries = client.list_directory(dirname)
                print(f"\n📁 Contents of '{dirname}':")
                if entries:
                    for entry in entries:
                        icon = "📁" if entry.get('is_directory') else "📄"
                        print(f"   {icon} {entry.get('name', 'unknown')}")
                else:
                    print("   (Empty directory)")
            
            elif choice == '9':
                dirname = input("Enter directory name to delete: ").strip()
                confirm = input(f"⚠️  Are you sure you want to delete directory '{dirname}'? (yes/no): ").strip().lower()
                if confirm == 'yes':
                    client.delete_directory(dirname)
                    print(f"✅ Directory '{dirname}' deleted!")
                else:
                    print("❌ Deletion cancelled")
            
            elif choice == '10':
                status = client.get_cluster_status()
                print(f"\n📊 Cluster Status:")
                print(f"   Primary: {status['primary_master']['master_id']} (Term {status['primary_master']['term']})")
                for master in status['all_masters']:
                    role = "PRIMARY ⭐" if master['is_primary'] else "BACKUP"
                    print(f"   - {master['master_id']}: {role}")
            
            else:
                print("❌ Invalid choice! Please enter 0-10")
        
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("   (Make sure the file/directory exists and try again)")

if __name__ == '__main__':
    main()
