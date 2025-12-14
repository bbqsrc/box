// Extension entry point that bridges to Rust.
// This file initializes the Rust library and lets FSKit discover the BoxFS class.

#import <Foundation/Foundation.h>

// Rust library entry point
extern void fskitbox_extension_main(void);

int main(int argc, const char * argv[]) {
    @autoreleasepool {
        // Initialize the Rust library
        fskitbox_extension_main();

        // Run the extension's main loop
        // FSKit extensions use NSXPCListener internally
        [[NSRunLoop currentRunLoop] run];
    }
    return 0;
}
