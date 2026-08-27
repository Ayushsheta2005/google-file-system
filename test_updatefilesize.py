#!/usr/bin/env python3

import sys
import time
from client import MultiMasterGFSClient

# Test the UpdateFileSize fix
def test_file_size_fix():
    print("🧪 Testing UpdateFileSize fix...")
    
    # Connect to GFS cluster
    client = MultiMasterGFSClient(['localhost:9010', 'localhost:9011', 'localhost:9012'])
    
    # Test 1: Create file and write data
    print("\n📝 Test 1: Create file and write data")
    filename = "test_updatefilesize.txt"
    test_data = "Hello, this is a test of the UpdateFileSize fix!"
    
    # Create file
    if not client.create_file(filename):
        print(f"❌ Failed to create file {filename}")
        return False
    print(f"✅ Created file: {filename}")
    
    # Write data
    if not client.write_file(filename, test_data.encode('utf-8')):
        print(f"❌ Failed to write data to {filename}")
        return False
    print(f"✅ Wrote {len(test_data)} bytes to {filename}")
    
    # Get file info to check size
    file_info = client.get_file_info(filename)
    if not file_info:
        print(f"❌ Failed to get file info for {filename}")
        return False
    
    print(f"📊 File info: size={file_info['size']} bytes, chunks={len(file_info['chunks'])}")
    
    # Check if file size is correct
    expected_size = len(test_data)
    actual_size = file_info['size']
    
    if actual_size == expected_size:
        print(f"✅ SUCCESS: File size is correct! Expected: {expected_size}, Actual: {actual_size}")
    else:
        print(f"❌ FAILED: File size mismatch! Expected: {expected_size}, Actual: {actual_size}")
        return False
    
    # Test 2: Read the data back
    print(f"\n📖 Test 2: Read data back from {filename}")
    read_data = client.read_file(filename)
    
    if read_data is None:
        print(f"❌ Failed to read data from {filename}")
        return False
    
    read_text = read_data.decode('utf-8')
    print(f"📄 Read {len(read_data)} bytes: '{read_text}'")
    
    if read_text == test_data:
        print(f"✅ SUCCESS: Read data matches written data!")
    else:
        print(f"❌ FAILED: Read data doesn't match! Expected: '{test_data}', Got: '{read_text}'")
        return False
    
    # Test 3: Test middle-of-file write (the complex case)
    print(f"\n✏️  Test 3: Write to middle of file (data shifting test)")
    insert_data = " INSERTED"
    insert_offset = 6  # Insert after "Hello,"
    
    if not client.write_file(filename, insert_data.encode('utf-8'), insert_offset):
        print(f"❌ Failed to write data at offset {insert_offset}")
        return False
    print(f"✅ Inserted '{insert_data}' at offset {insert_offset}")
    
    # Check new file size
    file_info = client.get_file_info(filename)
    if not file_info:
        print(f"❌ Failed to get updated file info")
        return False
    
    new_expected_size = len(test_data) + len(insert_data)
    new_actual_size = file_info['size']
    
    print(f"📊 Updated file info: size={new_actual_size} bytes")
    
    if new_actual_size == new_expected_size:
        print(f"✅ SUCCESS: Updated file size is correct! Expected: {new_expected_size}, Actual: {new_actual_size}")
    else:
        print(f"❌ FAILED: Updated file size mismatch! Expected: {new_expected_size}, Actual: {new_actual_size}")
        return False
    
    # Read back the modified data
    modified_data = client.read_file(filename)
    if modified_data:
        modified_text = modified_data.decode('utf-8')
        expected_text = test_data[:insert_offset] + insert_data + test_data[insert_offset:]
        print(f"📄 Modified file content: '{modified_text}'")
        print(f"📄 Expected content: '{expected_text}'")
        
        if modified_text == expected_text:
            print(f"✅ SUCCESS: Middle-of-file write with data shifting works correctly!")
        else:
            print(f"❌ FAILED: Data shifting didn't work correctly!")
            return False
    
    print(f"\n🎉 ALL TESTS PASSED! UpdateFileSize fix is working correctly!")
    return True

if __name__ == "__main__":
    if test_file_size_fix():
        print(f"\n🎯 CONCLUSION: The file size issue has been completely fixed!")
        sys.exit(0)
    else:
        print(f"\n💥 CONCLUSION: The file size issue still exists!")
        sys.exit(1)
