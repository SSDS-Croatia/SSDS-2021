// SPDX-License-Identifier: GPL-3.0

pragma solidity >=0.7.0 <0.9.0;

/**
 * @title Storage
 * @dev Store & retrieve value in a variable
 */
contract Storage {

    uint256 number;
    
    uint256 current_fee = 0.0001 ether;
    uint256 max_fee = 0.01 ether;
    
    struct NumberOwner {
        uint256 number;
        address owner;
    }
    
    address payable contract_owner;
    
    NumberOwner[] public number_owners;
    
    constructor() payable {
        contract_owner = payable(msg.sender);
    }

    /**
     * @dev Store value in variable
     * @param num value to store
     */
    function store(uint256 num, uint256 new_fee) public payable {
        require(msg.value >= current_fee);
        require(new_fee <= max_fee);
        
        current_fee = new_fee;
        
        NumberOwner memory newNumberOwner;
        newNumberOwner.number = num;
        newNumberOwner.owner = msg.sender;
        
        number = num;
        
        number_owners.push(newNumberOwner);
    }
    
    function withdraw() external payable onlyOwner {
        uint amount = address(this).balance;
        contract_owner.transfer(amount);
    }
    
    modifier onlyOwner() {
        require(msg.sender == contract_owner);
        _;
    }

    /**
     * @dev Return value 
     * @return value of 'number'
     */
    function retrieve() public view returns (uint256){
        return number;
    }
}
