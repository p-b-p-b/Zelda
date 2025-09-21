// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build arm64,!gccgo

#include "textflag.h"
DATA rotInvSRows<>+0x00(SB)/8, $0x080f0205040b0e01
DATA rotInvSRows<>+0x08(SB)/8, $0x00070a0d0c030609
GLOBL rotInvSRows<>(SB), (NOPTR+RODATA), $16
DATA invSRows<>+0x00(SB)/8, $0x0b0e0104070a0d00
DATA invSRows<>+0x08(SB)/8, $0x0306090c0f020508
GLOBL invSRows<>(SB), (NOPTR+RODATA), $16
// func ·encryptAes128(xk *uint32, dst, src *byte)
TEXT ·encryptAes128(SB),NOSPLIT,$0
	MOVD	xk+0(FP), R10
	MOVD	dst+8(FP), R11
	MOVD	src+16(FP), R12

	VLD1	(R12), [V0.B16]

	VLD1.P	64(R10), [V5.B16, V6.B16, V7.B16, V8.B16]
	VLD1.P	64(R10), [V9.B16, V10.B16, V11.B16, V12.B16]
	VLD1.P	48(R10), [V13.B16, V14.B16, V15.B16]
	AESE	V5.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V6.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V7.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V8.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V9.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V10.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V11.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V12.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V13.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V14.B16, V0.B16
	VEOR    V0.B16, V15.B16, V0.B16
	VST1	[V0.B16], (R11)
	RET

    // func aes128MMO(xk *uint32, dst, src *byte)
TEXT ·aes128MMO(SB),NOSPLIT,$0
	MOVD	xk+0(FP), R10
	MOVD	dst+8(FP), R11
	MOVD	src+16(FP), R12

	VLD1	(R12), [V0.B16]

	VLD1.P	64(R10), [V5.B16, V6.B16, V7.B16, V8.B16]
	VLD1.P	64(R10), [V9.B16, V10.B16, V11.B16, V12.B16]
	VLD1.P	48(R10), [V13.B16, V14.B16, V15.B16]
	AESE	V5.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V6.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V7.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V8.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V9.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V10.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V11.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V12.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V13.B16, V0.B16
	AESMC	V0.B16, V0.B16
	AESE	V14.B16, V0.B16
	VEOR    V0.B16, V15.B16, V0.B16
    VLD1	(R12), [V5.B16]
    VEOR    V0.B16, V5.B16, V0.B16
	VST1	[V0.B16], (R11)
	RET


// func expandKeyAsm(key *byte, enc *uint32) {
// Note that round keys are stored in uint128 format, not uint32
TEXT ·expandKeyAsm(SB),NOSPLIT,$0
	MOVD	key+0(FP), R9
	MOVD	enc+8(FP), R10
    MOVD    $10,R8
	LDP	rotInvSRows<>(SB), (R0, R1)
	VMOV	R0, V3.D[0]
	VMOV	R1, V3.D[1]
	VEOR	V0.B16, V0.B16, V0.B16 // All zeroes
	MOVW	$1, R13
	LDPW	(R9), (R4, R5)
	LDPW	8(R9), (R6, R7)
	STPW.P	(R4, R5), 8(R10)
	STPW.P	(R6, R7), 8(R10)
	MOVW	$0x1b, R14
ks128Loop:
		VMOV	R7, V2.S[0]
		WORD	$0x4E030042       // TBL V3.B16, [V2.B16], V2.B16
		AESE	V0.B16, V2.B16    // Use AES to compute the SBOX
		EORW	R13, R4
		LSLW	$1, R13           // Compute next Rcon
		ANDSW	$0x100, R13, ZR
		CSELW	NE, R14, R13, R13 // Fake modulo
		SUBS	$1, R8
		VMOV	V2.S[0], R0
		EORW	R0, R4
		EORW	R4, R5
		EORW	R5, R6
		EORW	R6, R7
		STPW.P	(R4, R5), 8(R10)
		STPW.P	(R6, R7), 8(R10)
	BNE	ks128Loop
ksDone:
	RET

// func xor16(dst, a, b *byte)
TEXT ·xor16(SB), NOSPLIT|NOFRAME, $0
	MOVD	dst+0(FP), R0
	MOVD	a+8(FP), R1
	MOVD	b+16(FP), R2
	LDP.P	16(R1), (R11, R12)
	LDP.P	16(R2), (R13, R14)
	EOR	R11, R13, R13
	EOR	R12, R14, R14
	STP.P	(R13, R14), 16(R0)
	RET

TEXT ·xorSlices(SB), $0-48
    MOVQ a+0(FP), SI   // SI = &a
    MOVQ b+24(FP), DI  // DI = &b
    MOVQ n+40(FP), BX  // BX = n
    SHRQ $3, BX        // BX /= 8 (process 8 elements per iteration)
    CMPQ BX, $0        // Check if BX is 0
    JE end             // If so, exit

    XORQ CX, CX        // CX = 0 (loop counter)

loop:
    MOVOU (SI)(CX*8), X0   // Load 8 elements from a into X0
    MOVOU (DI)(CX*8), X1   // Load 8 elements from b into X1
    PXOR X1, X0             // XOR X0 and X1, store result in X0
    MOVOU X0, (SI)(CX*8)   // Store the result back in a

    ADDQ $8, CX         // Increment loop counter by 8
    CMPQ CX, BX         // Check if we've processed all elements
    JNE loop            // If not, continue the loop

end:
    RET

TEXT ·xorSlicesRaw(SB), $0-56
    MOVQ dst+0(FP), SI         // Load pointer to dst slice
    MOVQ src+24(FP), DI        // Load pointer to src slice
    MOVQ n+32(FP), CX          // Load number of elements to process into CX

    // Calculate the number of 256-bit chunks (4 uint64 elements per chunk)
    SHRQ $2, CX                // Divide CX by 4 because we process 4 elements per iteration

loop:
    TESTQ CX, CX               // Test if the loop counter is zero
    JZ    done                 // If zero, we're done

    VMOVDQU (SI), Y0         // Load 256 bits from dst into YMM0
    VMOVDQU (DI), Y1         // Load 256 bits from src into YMM1
    VPXOR Y1, Y0, Y0     // Perform XOR operation between YMM1 and YMM0, result in YMM0
    VMOVDQU Y0, (SI)         // Store result back to dst from YMM0

    ADDQ $32, SI               // Advance pointers by 256 bits (32 bytes)
    ADDQ $32, DI
    DECQ CX                    // Decrement loop counter
    JNZ  loop                  // Continue if not done

done:
    VZEROUPPER                 // Clear upper part of YMM registers to avoid AVX-SSE transition penalty
    RET                        // Return
